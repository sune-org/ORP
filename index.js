import OpenAI from 'openai';
import Anthropic from '@anthropic-ai/sdk';
import { OpenRouter } from '@openrouter/sdk';

const TTL_MS = 20 * 60 * 1000;
const BATCH_MS = 800;
const BATCH_BYTES = 3400;
const HB_INTERVAL_MS = 3000;
const MAX_RUN_MS = 9 * 60 * 1000;

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Max-Age': '86400',
};

const withCORS = (resp) => {
  const headers = new Headers(resp.headers);
  Object.entries(CORS_HEADERS).forEach(([k, v]) => headers.set(k, v));
  return new Response(resp.body, { ...resp, headers });
};

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const method = req.method.toUpperCase();
    if (method === 'OPTIONS') return new Response(null, { status: 204, headers: CORS_HEADERS });
    if ((h => h !== 'sune.planetrenox.com' && h !== 'sune.chat' && !h.endsWith('.github.io'))(new URL(req.headers.get('Origin') || 'null').hostname)) return withCORS(new Response('Forbidden', { status: 403 }));
    if (url.pathname === '/ws') {
      const isGet = method === 'GET', isWs = req.headers.get('Upgrade') === 'websocket';
      if (!isGet && !isWs) return withCORS(new Response('method not allowed', { status: 405 }));
      const uid = (url.searchParams.get('uid') || '').slice(0, 64).replace(/[^a-zA-Z0-9_-]/g, '');
      if (!uid) return withCORS(new Response('uid is required', { status: 400 }));
      const id = env.MY_DURABLE_OBJECT.idFromName(uid), stub = env.MY_DURABLE_OBJECT.get(id);
      const resp = await stub.fetch(req);
      return isWs ? resp : withCORS(resp);
    }
    return withCORS(new Response('not found', { status: 404 }));
  }
}

export class MyDurableObject {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.sockets = new Set();
    this.reset();
  }

  reset() {
    this.rid = null;
    this.buffer = [];
    this.seq = -1;
    this.phase = 'idle';
    this.error = null;
    this.controller = null;
    this.oaStream = null;
    this.pending = '';
    this.flushTimer = null;
    this.lastSavedAt = 0;
    this.lastFlushedAt = 0;
    this.hbActive = false;
    this.age = 0;
    this.messages = [];
  }

  corsJSON(obj, status = 200) { return new Response(JSON.stringify(obj), { status, headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store', ...CORS_HEADERS } }); }
  send(ws, obj) { try { ws.send(JSON.stringify(obj)); } catch {} }
  bcast(obj) { this.sockets.forEach(ws => this.send(ws, obj)); }

  async autopsy() {
    if (this.rid) return;
    const snap = await this.state.storage.get('run').catch(() => null);
    if (!snap || (Date.now() - (snap.savedAt || 0) >= TTL_MS)) {
      if (snap) await this.state.storage.deleteAll();
      return;
    }
    this.rid = snap.rid;
    this.seq = snap.seq ?? -1;
    this.age = snap.age || 0;
    this.phase = snap.phase || 'done';
    this.error = snap.error || null;
    this.messages = snap.messages || [];
    const chunks = await this.state.storage.list({ prefix: 'c:' });
    this.buffer = Array.from(chunks.values()).sort((a, b) => a.seq - b.seq);
    if (this.phase === 'running') {
      this.phase = 'evicted';
      this.error = 'System eviction interrupted the run.';
      this.saveSnapshot();
      await this.stopHeartbeat();
    }
  }

  saveSnapshot() {
    this.lastSavedAt = Date.now();
    return this.state.storage.put('run', { rid: this.rid, seq: this.seq, age: this.age, phase: this.phase, error: this.error, savedAt: this.lastSavedAt, messages: this.messages });
  }

  replay(ws, after) {
    this.buffer.forEach(it => { if (it.seq > after) this.send(ws, { type: 'delta', seq: it.seq, text: it.t, images: it.i }); });
    if (/done/.test(this.phase)) this.send(ws, { type: 'done' });
    else if (/error|evicted/.test(this.phase)) this.send(ws, { type: 'err', message: this.error });
  }

  flush(force = false, imgs = null) {
    if (this.flushTimer) { clearTimeout(this.flushTimer); this.flushTimer = null; }
    if (this.pending || imgs) {
      const chunk = { seq: ++this.seq, t: this.pending, i: imgs };
      this.buffer.push(chunk);
      this.state.storage.put('c:' + chunk.seq, chunk);
      this.bcast({ type: 'delta', seq: chunk.seq, text: chunk.t, images: chunk.i });
      this.pending = '';
      this.lastFlushedAt = Date.now();
    }
    if (force) this.saveSnapshot();
  }

  queueDelta(text, imgs = null) {
    if (imgs) return this.flush(false, imgs);
    if (!text) return;
    this.pending += text;
    if (this.pending.length >= BATCH_BYTES) this.flush();
    else if (!this.flushTimer) this.flushTimer = setTimeout(() => this.flush(), BATCH_MS);
  }

  async fetch(req) {
    if (req.headers.get('Upgrade') === 'websocket') {
      const [client, server] = Object.values(new WebSocketPair());
      server.accept();
      this.sockets.add(server);
      server.addEventListener('close', () => this.sockets.delete(server));
      server.addEventListener('message', e => this.state.waitUntil(this.onMessage(server, e)));
      return new Response(null, { status: 101, webSocket: client });
    }
    await this.autopsy();
    const isTerminal = /done|error|evicted/.test(this.phase);
    return this.corsJSON({ rid: this.rid, seq: this.seq, phase: this.phase, done: isTerminal, error: this.error, text: this.buffer.map(it => it.t).join('') + this.pending, images: this.buffer.flatMap(it => it.i || []) });
  }

  async onMessage(ws, evt) {
    await this.autopsy();
    let msg; try { msg = JSON.parse(String(evt.data || '')); } catch { return this.send(ws, { type: 'err', message: 'bad_json' }); }
    if (msg.type === 'stop') { if (msg.rid === this.rid) this.stop(); return; }
    if (msg.type !== 'begin') return this.send(ws, { type: 'err', message: 'bad_type' });
    const { rid, apiKey, or_body, provider, after } = msg;
    const body = or_body || (msg.model && msg.messages ? { ...msg, stream: true } : null);
    if (!rid || !apiKey || !body) return this.send(ws, { type: 'err', message: 'missing_fields' });
    if (this.phase === 'running' && rid !== this.rid) return this.send(ws, { type: 'err', message: 'busy' });
    if (rid === this.rid && this.phase !== 'idle') return this.replay(ws, after ?? -1);
    await this.state.storage.deleteAll();
    this.reset();
    this.rid = rid;
    this.phase = 'running';
    this.controller = new AbortController();
    this.messages = body.messages;
    await this.saveSnapshot();
    this.state.waitUntil(this.startHeartbeat());
    this.state.waitUntil(this.stream({ apiKey, body, provider: provider || 'openrouter' }));
  }

  async stream({ apiKey, body, provider }) {
    try {
      const m = { openai: this.streamOpenAI, google: this.streamGoogle, claude: this.streamClaude };
      await (m[provider] || this.streamOpenRouter).call(this, { apiKey, body });
    } catch (e) {
      if (this.phase === 'running' && !/abort/i.test(e.message)) this.fail(e.message);
    } finally {
      if (this.phase === 'running') this.stop();
    }
  }

  async streamOpenRouter({ apiKey, body }) {
    const client = new OpenRouter({ apiKey, defaultHeaders: { 'HTTP-Referer': 'https://sune.chat', 'X-Title': 'Sune' } });
    const stream = await client.chat.send({ ...body, stream: true });
    let hasR = false, hasC = false;
    for await (const chunk of stream) {
      if (this.phase !== 'running') break;
      const d = chunk?.choices?.[0]?.delta;
      if (d?.reasoning && body.reasoning?.exclude !== true) { this.queueDelta(d.reasoning); hasR = true; }
      if (d?.content) { if (hasR && !hasC) this.queueDelta('\n'); this.queueDelta(d.content); hasC = true; }
      if (d?.images) this.queueDelta('', d.images);
    }
  }

  async streamOpenAI({ apiKey, body }) {
    const client = new OpenAI({ apiKey });
    const params = { model: body.model, input: this.buildInputForResponses(body.messages), temperature: body.temperature, stream: true };
    if (body.reasoning?.effort) params.reasoning = { effort: body.reasoning.effort };
    this.oaStream = await client.responses.stream(params);
    for await (const event of this.oaStream) {
      if (this.phase !== 'running') break;
      if (event.type.endsWith('.delta') && event.delta) this.queueDelta(event.delta);
    }
  }

  async streamClaude({ apiKey, body }) {
    const client = new Anthropic({ apiKey });
    const sys = body.messages.filter(m => m.role === 'system').map(m => this.extractTextFromMessage(m)).join('\n') || body.system;
    const payload = { model: body.model, system: sys, messages: body.messages.filter(m => m.role !== 'system').map(m => ({ role: m.role, content: typeof m.content === 'string' ? m.content : (m.content || []).map(p => p.type === 'text' ? { type: 'text', text: p.text } : (p.type === 'image_url' ? { type: 'image', source: { type: 'base64', media_type: p.image_url.url.match(/:(.*?);/)[1], data: p.image_url.url.split(',')[1] } } : null)).filter(Boolean) })), max_tokens: body.max_tokens || 4096 };
    const stream = client.messages.stream(payload);
    stream.on('text', t => this.phase === 'running' && this.queueDelta(t));
    await stream.finalMessage();
  }

  async streamGoogle({ apiKey, body }) {
    const model = (body.model ?? '').replace(/:online$/, '');
    const payload = { contents: this.mapToGoogleContents(body.messages), generationConfig: { temperature: body.temperature, topP: body.top_p } };
    const resp = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${model}:streamGenerateContent?alt=sse`, { method: 'POST', headers: { 'Content-Type': 'application/json', 'x-goog-api-key': apiKey }, body: JSON.stringify(payload), signal: this.controller.signal });
    const reader = resp.body.getReader(), dec = new TextDecoder();
    let buf = '';
    while (this.phase === 'running') {
      const { done, value } = await reader.read(); if (done) break;
      buf += dec.decode(value, { stream: true });
      const lines = buf.split('\n'); buf = lines.pop();
      for (const l of lines) {
        if (!l.startsWith('data: ')) continue;
        try { JSON.parse(l.slice(6))?.candidates?.[0]?.content?.parts?.forEach(p => p.text && this.queueDelta(p.text)); } catch {}
      }
    }
  }

  stop() { if (this.phase === 'running') { this.flush(true); this.phase = 'done'; this.bcast({ type: 'done' }); this.state.waitUntil(this.stopHeartbeat()); } }
  fail(m) { if (this.phase === 'running') { this.flush(true); this.phase = 'error'; this.error = String(m); this.bcast({ type: 'err', message: this.error }); this.state.waitUntil(this.stopHeartbeat()); } }
  async startHeartbeat() { this.hbActive = true; await this.state.storage.setAlarm(Date.now() + HB_INTERVAL_MS); }
  async stopHeartbeat() { this.hbActive = false; await this.state.storage.setAlarm(null); }
  async alarm() { await this.autopsy(); if (this.phase === 'running' && ++this.age * HB_INTERVAL_MS < MAX_RUN_MS) await this.state.storage.setAlarm(Date.now() + HB_INTERVAL_MS); else if (this.phase === 'running') this.fail('timeout'); }

  extractTextFromMessage(m) { return typeof m.content === 'string' ? m.content : (m.content || []).filter(p => p.type === 'text').map(p => p.text).join(''); }
  buildInputForResponses(msgs) { return msgs.map(m => ({ role: m.role, content: this.extractTextFromMessage(m) })); }
  mapToGoogleContents(msgs) { return msgs.filter(m => m.role !== 'system').map(m => ({ role: m.role === 'assistant' ? 'model' : 'user', parts: [{ text: this.extractTextFromMessage(m) }] })); }
}
