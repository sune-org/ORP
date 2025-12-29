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
    if ((h => h !== 'sune.planetrenox.com' && h !== 'sune.chat' && !h.endsWith('.github.io'))(new URL(req.headers.get('Origin') || 'null').hostname)) {
      return withCORS(new Response('Forbidden', { status: 403 }));
    }
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
    this.rid = null; this.buffer = []; this.seq = -1; this.phase = 'idle'; this.error = null;
    this.controller = null; this.oaStream = null; this.pending = ''; this.pendingImages = [];
    this.flushTimer = null; this.lastSavedAt = 0; this.lastFlushedAt = 0; this.hbActive = false;
    this.age = 0; this.messages = [];
  }

  corsJSON(obj, status = 200) {
    return new Response(JSON.stringify(obj), { status, headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store', ...CORS_HEADERS } });
  }

  send(ws, obj) { try { ws.send(JSON.stringify(obj)); } catch {} }
  bcast(obj) { this.sockets.forEach(ws => this.send(ws, obj)); }

  notify(msg, pri = 3, tags = []) {
    if (!this.env.NTFY_URL) return;
    this.state.waitUntil(fetch(this.env.NTFY_URL, {
      method: 'POST', body: msg, headers: { Title: 'Sune ORP', Priority: `${pri}`, Tags: tags.join(',') }
    }).catch(e => console.error('ntfy failed:', e)));
  }

  async autopsy() {
    if (this.rid) return;
    const snap = await this.state.storage.get('run').catch(() => null);
    if (!snap || (Date.now() - (snap.savedAt || 0) >= TTL_MS)) {
      if (snap) await this.state.storage.delete('run').catch(() => {});
      return;
    }
    this.rid = snap.rid; this.buffer = snap.buffer || []; this.seq = +snap.seq || -1;
    this.age = snap.age || 0; this.phase = snap.phase || 'done'; this.error = snap.error;
    this.messages = snap.messages || []; this.pending = ''; this.pendingImages = [];
    if (this.phase === 'running') {
      this.phase = 'evicted'; this.error = 'System eviction interrupted the run.';
      this.saveSnapshot(); this.notify(`Run ${this.rid} evicted`, 4, ['warning']);
      await this.stopHeartbeat();
    }
  }

  saveSnapshot() {
    this.lastSavedAt = Date.now();
    return this.state.storage.put('run', { rid: this.rid, buffer: this.buffer, seq: this.seq, age: this.age, phase: this.phase, error: this.error, savedAt: this.lastSavedAt, messages: this.messages }).catch(() => {});
  }

  replay(ws, after) {
    this.buffer.forEach(it => { if (it.seq > after) this.send(ws, { type: 'delta', seq: it.seq, text: it.text, images: it.images }); });
    if (this.phase === 'done') this.send(ws, { type: 'done' });
    else if (['error', 'evicted'].includes(this.phase)) this.send(ws, { type: 'err', message: this.error || 'Terminated unexpectedly.' });
  }

  flush(force = false) {
    if (this.flushTimer) { clearTimeout(this.flushTimer); this.flushTimer = null; }
    if (this.pending || this.pendingImages.length > 0) {
      const item = { seq: ++this.seq, text: this.pending };
      if (this.pendingImages.length > 0) item.images = [...this.pendingImages];
      this.buffer.push(item);
      this.bcast({ type: 'delta', seq: this.seq, text: this.pending, images: item.images });
      this.pending = ''; this.pendingImages = []; this.lastFlushedAt = Date.now();
    }
    if (force) this.saveSnapshot();
  }

  queueDelta(text, images) {
    if (!text && (!images || !images.length)) return;
    if (text) this.pending += text;
    if (images) this.pendingImages.push(...images);
    if (this.pending.length >= BATCH_BYTES || this.pendingImages.length > 0) this.flush(false);
    else if (!this.flushTimer) this.flushTimer = setTimeout(() => this.flush(false), BATCH_MS);
  }

  async fetch(req) {
    if (req.method === 'OPTIONS') return new Response(null, { status: 204, headers: CORS_HEADERS });
    if (req.headers.get('Upgrade') === 'websocket') {
      const [client, server] = Object.values(new WebSocketPair());
      server.accept(); this.sockets.add(server);
      server.addEventListener('close', () => this.sockets.delete(server));
      server.addEventListener('message', e => this.state.waitUntil(this.onMessage(server, e)));
      return new Response(null, { status: 101, webSocket: client });
    }
    if (req.method === 'GET') {
      await this.autopsy();
      const text = this.buffer.map(it => it.text).join('') + this.pending;
      const images = [...this.buffer.flatMap(it => it.images || []), ...this.pendingImages];
      const isTerminal = ['done', 'error', 'evicted'].includes(this.phase);
      return this.corsJSON({ rid: this.rid, seq: this.seq, phase: this.phase, done: isTerminal, error: ['error', 'evicted'].includes(this.phase) ? (this.error || 'Terminated.') : null, text, images });
    }
    return this.corsJSON({ error: 'not allowed' }, 405);
  }

  async onMessage(ws, evt) {
    await this.autopsy();
    let msg; try { msg = JSON.parse(String(evt.data || '')); } catch { return this.send(ws, { type: 'err', message: 'bad_json' }); }
    if (msg.type === 'stop') { if (msg.rid === this.rid) this.stop(); return; }
    if (msg.type !== 'begin') return this.send(ws, { type: 'err', message: 'bad_type' });
    const { rid, apiKey, or_body, model, messages, after, provider } = msg;
    const body = or_body || (model && Array.isArray(messages) ? { model, messages, stream: true, ...msg } : null);
    if (!rid || !apiKey || !body || !body.messages?.length) return this.send(ws, { type: 'err', message: 'missing_fields' });
    if (this.phase === 'running' && rid !== this.rid) return this.send(ws, { type: 'err', message: 'busy' });
    if (rid === this.rid && this.phase !== 'idle') return this.replay(ws, Number.isFinite(+after) ? +after : -1);
    this.reset(); this.rid = rid; this.phase = 'running'; this.controller = new AbortController(); this.messages = body.messages;
    await this.saveSnapshot();
    this.state.waitUntil(this.startHeartbeat());
    this.state.waitUntil(this.stream({ apiKey, body, provider: provider || 'openrouter' }));
  }

  async stream({ apiKey, body, provider }) {
    try {
      const providerMap = { openai: this.streamOpenAI, google: this.streamGoogle, claude: this.streamClaude };
      await (providerMap[provider] || this.streamOpenRouter).call(this, { apiKey, body });
    } catch (e) {
      if (this.phase === 'running') {
        const msg = String(e?.message || 'stream_failed');
        if (!((e && e.name === 'AbortError') || /abort/i.test(msg))) this.fail(msg);
      }
    } finally { if (this.phase === 'running') this.stop(); }
  }

  async streamOpenAI({ apiKey, body }) {
    const client = new OpenAI({ apiKey });
    const params = { model: body.model, input: this.buildInputForResponses(body.messages || []), temperature: body.temperature, stream: true };
    if (body.reasoning?.effort) params.reasoning = { effort: body.reasoning.effort };
    this.oaStream = await client.responses.stream(params);
    for await (const event of this.oaStream) {
      if (this.phase !== 'running') break;
      if (event.type.endsWith('.delta') && event.delta) this.queueDelta(event.delta);
    }
  }

  async streamClaude({ apiKey, body }) {
    const client = new Anthropic({ apiKey });
    const system = body.messages.filter(m => m.role === 'system').map(m => this.extractTextFromMessage(m)).join('\n\n') || body.system;
    const payload = {
      model: body.model, max_tokens: body.max_tokens || 64000,
      messages: body.messages.filter(m => m.role !== 'system').map(m => ({
        role: m.role, content: (Array.isArray(m.content) ? m.content : [{type:'text',text:String(m.content)}]).map(p => {
          if (p.type === 'text') return { type: 'text', text: p.text };
          if (p.type === 'image_url') {
            const m = String(p.image_url?.url || '').match(/^data:(image\/\w+);base64,(.*)$/);
            if (m) return { type: 'image', source: { type: 'base64', media_type: m[1], data: m[2] } };
          }
        }).filter(Boolean)
      })).filter(m => m.content.length)
    };
    if (system) payload.system = system;
    const stream = client.messages.stream(payload);
    stream.on('text', text => { if (this.phase === 'running') this.queueDelta(text); });
    await stream.finalMessage();
  }
  
  async streamGoogle({ apiKey, body }) {
    const model = (body.model ?? '').replace(/:online$/, '');
    const payload = { contents: this.mapToGoogleContents(body.messages) };
    const resp = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${model}:streamGenerateContent?alt=sse`, { method: 'POST', headers: { 'Content-Type': 'application/json', 'x-goog-api-key': apiKey }, body: JSON.stringify(payload), signal: this.controller.signal });
    if (!resp.ok) throw new Error(`Google API error: ${resp.status}`);
    const reader = resp.body.getReader(), decoder = new TextDecoder();
    let buffer = '';
    while (this.phase === 'running') {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      for (const line of buffer.split('\n')) {
        if (!line.startsWith('data: ')) continue;
        try {
          JSON.parse(line.substring(6))?.candidates?.[0]?.content?.parts?.forEach(p => {
            if (p.thought?.thought) this.queueDelta(p.thought.thought);
            if (p.text) this.queueDelta(p.text);
          });
        } catch {}
      }
      buffer = buffer.slice(buffer.lastIndexOf('\n') + 1);
    }
  }

  async streamOpenRouter({ apiKey, body }) {
    const client = new OpenRouter({ apiKey, defaultHeaders: { 'HTTP-Referer': 'https://sune.chat', 'X-Title': 'Sune' } });
    this.queueDelta(`> [DEBUG] Requesting ${body.model} (modalities: ${JSON.stringify(body.modalities || 'default')})\n\n`);
    try {
      const stream = await client.chat.send({ ...body, stream: true });
      let hasReasoning = false, hasContent = false, collectedImages = [];
      for await (const chunk of stream) {
        if (this.phase !== 'running') break;
        const delta = chunk?.choices?.[0]?.delta;
        if (!delta) continue;
        if (delta.reasoning && body.reasoning?.exclude !== true) { this.queueDelta(delta.reasoning); hasReasoning = true; }
        if (delta.content) { if (hasReasoning && !hasContent) this.queueDelta('\n'); this.queueDelta(delta.content); hasContent = true; }
        if (Array.isArray(delta.images)) collectedImages.push(...delta.images);
      }
      if (collectedImages.length) this.queueDelta('', collectedImages);
      else if (!hasContent) this.queueDelta(`> [DEBUG] Stream ended with no content/images. Check if model supports streaming for this modality.`);
    } catch (e) { this.queueDelta(`\n\n> [DEBUG] OR Error: ${e.message}`); throw e; }
  }

  stop() {
    if (this.phase !== 'running') return;
    this.flush(true); this.phase = 'done'; this.error = null;
    try { this.controller?.abort(); } catch {}
    try { this.oaStream?.controller?.abort(); } catch {}
    this.saveSnapshot(); this.bcast({ type: 'done' });
    this.state.waitUntil(this.stopHeartbeat());
  }

  fail(message) {
    if (this.phase !== 'running') return;
    this.flush(true); this.phase = 'error'; this.error = String(message || 'stream_failed');
    try { this.controller?.abort(); } catch {}
    try { this.oaStream?.controller?.abort(); } catch {}
    this.saveSnapshot(); this.bcast({ type: 'err', message: this.error });
    this.notify(`Run ${this.rid} failed: ${this.error}`, 3, ['rotating_light']);
    this.state.waitUntil(this.stopHeartbeat());
  }

  async startHeartbeat() { if (!this.hbActive && this.phase === 'running') { this.hbActive = true; await this.state.storage.setAlarm(Date.now() + HB_INTERVAL_MS).catch(() => {}); } }
  async stopHeartbeat() { if (this.hbActive) { this.hbActive = false; this.notify(`Run ${this.rid} ended. Age: ${((this.age * HB_INTERVAL_MS) / 1000).toFixed(1)}s.`, 3, ['stop_sign']); await this.state.storage.setAlarm(null).catch(() => {}); } }
  async Heart() { if (this.phase !== 'running' || !this.hbActive) return this.stopHeartbeat(); if (++this.age * HB_INTERVAL_MS >= MAX_RUN_MS) return this.fail(`Timeout.`); await this.state.storage.setAlarm(Date.now() + HB_INTERVAL_MS).catch(() => {}); }
  async alarm() { await this.autopsy(); await this.Heart(); }
  
  isMultimodalMessage(m) { return m && Array.isArray(m.content) && m.content.some(p => p?.type && !['text', 'input_text'].includes(p.type)); }
  extractTextFromMessage(m) { if (!m) return ''; if (typeof m.content === 'string') return m.content; if (!Array.isArray(m.content)) return ''; return m.content.filter(p => p && ['text', 'input_text'].includes(p.type)).map(p => String(p.text ?? p.content ?? '')).join(''); }
  mapContentPartToResponses(p) { const t = p?.type || 'text'; if (['image_url', 'input_image'].includes(t)) return (p?.image_url?.url || p?.image_url) ? { type: 'input_image', image_url: String(p?.image_url?.url || p?.image_url) } : null; return { type: 'input_text', text: String(t === 'text' ? (p.text ?? p.content ?? '') : (p.text ?? '')) }; }
  buildInputForResponses(msgs) { if (!Array.isArray(msgs) || !msgs.length) return ''; if (!msgs.some(m => this.isMultimodalMessage(m))) return msgs.length === 1 ? this.extractTextFromMessage(msgs[0]) : msgs.map(m => ({ role: m.role, content: this.extractTextFromMessage(m) })); return msgs.map(m => ({ role: m.role, content: Array.isArray(m.content) ? m.content.map(p => this.mapContentPartToResponses(p)).filter(Boolean) : [{ type: 'input_text', text: String(m.content || '') }] })); }
  mapToGoogleContents(msgs) { const c = msgs.reduce((acc, m) => { const r = m.role === 'assistant' ? 'model' : 'user', p = (Array.isArray(m.content) ? m.content : [{ type: 'text', text: String(m.content ?? '') }]).map(p => { if (p.type === 'text') return { text: p.text || '' }; if (p.type === 'image_url' && p.image_url?.url) { const m = p.image_url.url.match(/^data:(image\/\w+);base64,(.*)$/); if (m) return { inline_data: { mime_type: m[1], data: m[2] } }; } return null; }).filter(Boolean); if (!p.length) return acc; if (acc.length && acc.at(-1).role === r) acc.at(-1).parts.push(...p); else acc.push({ role: r, parts: p }); return acc; }, []); if (c.at(-1)?.role !== 'user') c.pop(); return c; }
}
