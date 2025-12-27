import OpenAI from 'openai';
import Anthropic from '@anthropic-ai/sdk';
import { OpenRouter } from '@openrouter/sdk';

const TTL_MS = 20 * 60 * 1000, BATCH_MS = 800, BATCH_BYTES = 3400, HB_INTERVAL_MS = 3000, MAX_RUN_MS = 9 * 60 * 1000;
const CORS_HEADERS = { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type, Authorization', 'Access-Control-Max-Age': '86400' };

const withCORS = r => {
  const h = new Headers(r.headers);
  Object.entries(CORS_HEADERS).forEach(([k, v]) => h.set(k, v));
  return new Response(r.body, { ...r, headers: h });
};

export default {
  async fetch(req, env) {
    const url = new URL(req.url), method = req.method.toUpperCase();
    if (method === 'OPTIONS') return new Response(null, { status: 204, headers: CORS_HEADERS });
    const origin = req.headers.get('Origin') || 'null';
    if (!['sune.planetrenox.com', 'sune.chat', 'localhost'].some(h => origin.includes(h)) && !origin.endsWith('.github.io')) return withCORS(new Response('Forbidden', { status: 403 }));
    if (url.pathname === '/ws') {
      const uid = (url.searchParams.get('uid') || '').slice(0, 64).replace(/[^a-zA-Z0-9_-]/g, '');
      if (!uid) return withCORS(new Response('uid required', { status: 400 }));
      const stub = env.MY_DURABLE_OBJECT.get(env.MY_DURABLE_OBJECT.idFromName(uid));
      const resp = await stub.fetch(req);
      return req.headers.get('Upgrade') === 'websocket' ? resp : withCORS(resp);
    }
    return withCORS(new Response('not found', { status: 404 }));
  }
}

export class MyDurableObject {
  constructor(state, env) {
    this.state = state; this.env = env; this.sockets = new Set(); this.reset();
  }

  reset() {
    this.rid = null; this.buffer = []; this.seq = -1; this.phase = 'idle'; this.error = null;
    this.controller = null; this.oaStream = null; this.pending = ''; this.pendingImgs = null;
    this.flushTimer = null; this.lastSavedAt = 0; this.hbActive = false; this.age = 0; this.messages = [];
  }

  corsJSON(obj, status = 200) { return new Response(JSON.stringify(obj), { status, headers: { 'Content-Type': 'application/json', ...CORS_HEADERS } }); }
  bcast(obj) { this.sockets.forEach(ws => { try { ws.send(JSON.stringify(obj)); } catch {} }); }

  async autopsy() {
    if (this.rid) return;
    const snap = await this.state.storage.get('run').catch(() => null);
    if (!snap || (Date.now() - (snap.savedAt || 0) >= TTL_MS)) {
      if (snap) { await this.state.storage.deleteAll(); this.reset(); }
      return;
    }
    Object.assign(this, { rid: snap.rid, seq: snap.seq || -1, age: snap.age || 0, phase: snap.phase || 'done', error: snap.error, messages: snap.messages || [] });
    const chunks = await this.state.storage.list({ prefix: 'c_' });
    this.buffer = Array.from(chunks.values());
    if (this.phase === 'running') {
      this.phase = 'evicted'; this.error = 'System eviction'; this.saveSnapshot(); this.stopHeartbeat();
    }
  }

  saveSnapshot() {
    this.lastSavedAt = Date.now();
    const snap = { rid: this.rid, seq: this.seq, age: this.age, phase: this.phase, error: this.error, savedAt: this.lastSavedAt, messages: this.messages };
    return this.state.storage.put('run', snap);
  }

  replay(ws, after) {
    this.buffer.filter(it => it.seq > after).forEach(it => ws.send(JSON.stringify({ type: 'delta', seq: it.seq, text: it.t, images: it.i })));
    if (this.phase === 'done') ws.send(JSON.stringify({ type: 'done' }));
    else if (['error', 'evicted'].includes(this.phase)) ws.send(JSON.stringify({ type: 'err', message: this.error }));
  }

  flush(force = false) {
    if (this.flushTimer) { clearTimeout(this.flushTimer); this.flushTimer = null; }
    if (this.pending || this.pendingImgs) {
      const chunk = { seq: ++this.seq, t: this.pending, i: this.pendingImgs };
      this.buffer.push(chunk);
      this.state.storage.put(`c_${this.seq.toString().padStart(6, '0')}`, chunk);
      this.bcast({ type: 'delta', ...chunk, text: chunk.t, images: chunk.i });
      this.pending = ''; this.pendingImgs = null;
    }
    if (force) this.saveSnapshot();
  }

  queueDelta(text, imgs) {
    if (imgs) { this.flush(); this.pendingImgs = imgs; this.flush(); return; }
    this.pending += (text || '');
    if (this.pending.length >= BATCH_BYTES) this.flush();
    else if (!this.flushTimer) this.flushTimer = setTimeout(() => this.flush(), BATCH_MS);
  }

  async fetch(req) {
    if (req.headers.get('Upgrade') === 'websocket') {
      const [c, s] = Object.values(new WebSocketPair());
      s.accept(); this.sockets.add(s);
      s.addEventListener('close', () => this.sockets.delete(s));
      s.addEventListener('message', e => this.state.waitUntil(this.onMessage(s, e)));
      return new Response(null, { status: 101, webSocket: c });
    }
    await this.autopsy();
    const text = this.buffer.map(it => it.t).join('') + this.pending;
    const imgs = this.buffer.flatMap(it => it.i || []);
    return this.corsJSON({ rid: this.rid, seq: this.seq, phase: this.phase, done: ['done', 'error', 'evicted'].includes(this.phase), text, images: imgs });
  }

  async onMessage(ws, evt) {
    await this.autopsy();
    let m; try { m = JSON.parse(evt.data); } catch { return; }
    if (m.type === 'stop' && m.rid === this.rid) return this.stop();
    if (m.type !== 'begin') return;
    const { rid, apiKey, or_body, provider, after } = m;
    if (this.phase === 'running' && rid !== this.rid) return ws.send(JSON.stringify({ type: 'err', message: 'busy' }));
    if (rid === this.rid && this.phase !== 'idle') return this.replay(ws, after ?? -1);
    this.reset(); this.rid = rid; this.phase = 'running'; this.controller = new AbortController();
    this.messages = or_body?.messages || [];
    await this.state.storage.deleteAll();
    await this.saveSnapshot();
    this.state.waitUntil(this.startHeartbeat());
    this.state.waitUntil(this.stream({ apiKey, body: or_body, provider: provider || 'openrouter' }));
  }

  async stream({ apiKey, body, provider }) {
    try {
      const map = { openai: this.streamOpenAI, google: this.streamGoogle, claude: this.streamClaude };
      await (map[provider] || this.streamOpenRouter).call(this, { apiKey, body });
    } catch (e) {
      if (this.phase === 'running') this.fail(e.message);
    } finally {
      if (this.phase === 'running') this.stop();
    }
  }

  async streamOpenRouter({ apiKey, body }) {
    const client = new OpenRouter({ apiKey });
    const stream = await client.chat.send({ ...body, stream: true });
    for await (const chunk of stream) {
      if (this.phase !== 'running') break;
      const d = chunk?.choices?.[0]?.delta;
      if (d?.reasoning && body.reasoning?.exclude !== true) this.queueDelta(d.reasoning);
      if (d?.content) this.queueDelta(d.content);
      if (d?.images) this.queueDelta('', d.images);
    }
  }

  async streamGoogle({ apiKey, body }) {
    const model = (body.model || '').replace(/:online$/, '');
    const resp = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${model}:streamGenerateContent?alt=sse`, {
      method: 'POST', headers: { 'Content-Type': 'application/json', 'x-goog-api-key': apiKey },
      body: JSON.stringify({ contents: this.mapToGoogle(body.messages) }), signal: this.controller.signal
    });
    const reader = resp.body.getReader();
    const dec = new TextDecoder();
    let buf = '';
    while (this.phase === 'running') {
      const { done, value } = await reader.read();
      if (done) break;
      buf += dec.decode(value, { stream: true });
      const lines = buf.split('\n'); buf = lines.pop();
      for (const l of lines) {
        if (!l.startsWith('data: ')) continue;
        try {
          JSON.parse(l.slice(6)).candidates?.[0]?.content?.parts?.forEach(p => {
            if (p.thought?.thought) this.queueDelta(p.thought.thought);
            if (p.text) this.queueDelta(p.text);
          });
        } catch {}
      }
    }
  }

  stop() { this.phase = 'done'; this.finish(); }
  fail(m) { this.phase = 'error'; this.error = m; this.finish(); }
  finish() { this.flush(true); this.controller?.abort(); this.bcast({ type: this.phase === 'done' ? 'done' : 'err', message: this.error }); this.stopHeartbeat(); }

  async startHeartbeat() { this.hbActive = true; await this.state.storage.setAlarm(Date.now() + HB_INTERVAL_MS); }
  async stopHeartbeat() { this.hbActive = false; await this.state.storage.setAlarm(null); }
  async alarm() {
    if (this.phase !== 'running' || !this.hbActive) return this.stopHeartbeat();
    if (++this.age * HB_INTERVAL_MS >= MAX_RUN_MS) return this.fail('Timeout');
    await this.state.storage.setAlarm(Date.now() + HB_INTERVAL_MS);
  }

  mapToGoogle(msgs) {
    return msgs.map(m => ({
      role: m.role === 'assistant' ? 'model' : 'user',
      parts: (Array.isArray(m.content) ? m.content : [{ type: 'text', text: String(m.content) }]).map(p => {
        if (p.type === 'text') return { text: p.text };
        if (p.type === 'image_url' && p.image_url?.url) {
          const m = p.image_url.url.match(/^data:(image\/\w+);base64,(.*)$/);
          if (m) return { inline_data: { mime_type: m[1], data: m[2] } };
        }
        return null;
      }).filter(Boolean)
    })).filter(m => m.parts.length);
  }
}
