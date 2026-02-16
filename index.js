import OpenAI from 'openai';
import Anthropic from '@anthropic-ai/sdk';

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
      const isGet = method === 'GET';
      const isWs = req.headers.get('Upgrade') === 'websocket';
      if (!isGet && !isWs) return withCORS(new Response('method not allowed', { status: 405 }));

      const uid = (url.searchParams.get('uid') || '').slice(0, 64).replace(/[^a-zA-Z0-9_-]/g, '');
      if (!uid) return withCORS(new Response('uid is required', { status: 400 }));
      
      const id = env.MY_DURABLE_OBJECT.idFromName(uid);
      const stub = env.MY_DURABLE_OBJECT.get(id);

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
    this.resetLocal();
  }

  resetLocal() {
    this.rid = null;
    this.buffer = [];
    this.seq = -1;
    this.phase = 'idle';
    this.error = null;
    this.controller = null;
    this.oaStream = null;
    this.pending = '';
    this.pendingImages = [];
    this.flushTimer = null;
    this.lastSavedAt = 0;
    this.lastFlushedAt = 0;
    this.hbActive = false;
    this.age = 0;
    this.messages = [];
  }

  async resetStorage() {
    await this.state.storage.deleteAll();
    this.resetLocal();
  }

  corsJSON(obj, status = 200) {
    return new Response(JSON.stringify(obj), { status, headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store', ...CORS_HEADERS } });
  }

  send(ws, obj) { try { ws.send(JSON.stringify(obj)); } catch {} }

  bcast(obj) { this.sockets.forEach(ws => this.send(ws, obj)); }

  notify(msg, pri = 3, tags = []) {
    if (!this.env.NTFY_URL) return;
    const headers = { Title: 'Sune ORP', Priority: `${pri}`, Tags: tags.join(',') };
    this.state.waitUntil(fetch(this.env.NTFY_URL, {
      method: 'POST',
      body: msg,
      headers,
    }).catch(e => console.error('ntfy failed:', e)));
  }

  async autopsy() {
    if (this.rid) return;
    const snap = await this.state.storage.get('run').catch(() => null);

    if (!snap || (Date.now() - (snap.savedAt || 0) >= TTL_MS)) {
      if (snap) await this.state.storage.deleteAll().catch(() => {});
      return;
    }

    this.rid = snap.rid || null;
    this.seq = Number.isFinite(+snap.seq) ? +snap.seq : -1;
    this.age = snap.age || 0;
    this.phase = snap.phase || 'done';
    this.error = snap.error || null;
    
    const [msgs, deltaMap] = await Promise.all([
      this.state.storage.get('prompt').catch(() => []),
      this.state.storage.list({ prefix: 'delta:' }).catch(() => new Map())
    ]);

    this.messages = Array.isArray(msgs) ? msgs : [];
    this.buffer = Array.from(deltaMap.values()).sort((a, b) => a.seq - b.seq);
    this.pending = '';
    this.pendingImages = [];

    if (this.phase === 'running') {
      this.phase = 'evicted';
      this.error = 'The run was interrupted due to system eviction.';
      await this.saveSnapshot();
      this.notify(`Run ${this.rid} evicted`, 4, ['warning']);
      await this.stopHeartbeat();
    }
  }

  saveSnapshot() {
    this.lastSavedAt = Date.now();
    const snapshot = { 
      rid: this.rid, 
      seq: this.seq, 
      age: this.age, 
      phase: this.phase, 
      error: this.error, 
      savedAt: this.lastSavedAt 
    };
    return this.state.storage.put('run', snapshot).catch(() => {});
  }

  replay(ws, after) {
    this.buffer.forEach(it => { if (it.seq > after) this.send(ws, { type: 'delta', seq: it.seq, text: it.text, images: it.images }); });
    if (this.phase === 'done') this.send(ws, { type: 'done' });
    else if (['error', 'evicted'].includes(this.phase)) this.send(ws, { type: 'err', message: this.error || 'The run was terminated unexpectedly.' });
  }

  flush(force = false) {
    if (this.flushTimer) { clearTimeout(this.flushTimer); this.flushTimer = null; }
    if (this.pending || this.pendingImages.length > 0) {
      const item = { seq: ++this.seq, text: this.pending };
      if (this.pendingImages.length > 0) item.images = [...this.pendingImages];
      
      this.buffer.push(item);
      this.bcast({ type: 'delta', seq: this.seq, text: this.pending, images: item.images });
      
      const key = `delta:${String(item.seq).padStart(10, '0')}`;
      this.state.storage.put(key, item).catch(() => {});

      this.pending = '';
      this.pendingImages = [];
      this.lastFlushedAt = Date.now();
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
      server.accept();
      this.sockets.add(server);
      server.addEventListener('close', () => this.sockets.delete(server));
      server.addEventListener('message', e => this.state.waitUntil(this.onMessage(server, e)));
      return new Response(null, { status: 101, webSocket: client });
    }

    if (req.method === 'GET') {
      await this.autopsy();
      const text = this.buffer.map(it => it.text).join('') + this.pending;
      const images = [...this.buffer.flatMap(it => it.images || []), ...this.pendingImages];
      const isTerminal = ['done', 'error', 'evicted'].includes(this.phase);
      const isError = ['error', 'evicted'].includes(this.phase);
      const payload = { rid: this.rid, seq: this.seq, phase: this.phase, done: isTerminal, error: isError ? (this.error || 'The run was terminated unexpectedly.') : null, text, images };
      return this.corsJSON(payload);
    }
    return this.corsJSON({ error: 'not allowed' }, 405);
  }

  sanitizeMessages(messages) {
    if (!Array.isArray(messages)) return [];
    return messages.map(m => {
      let content = m.content;
      if (typeof content === 'string') {
        if (!content.trim()) content = '.';
      } else if (Array.isArray(content)) {
        content = content.filter(p => p.type !== 'text' || (p.text && p.text.trim().length > 0));
        if (content.length === 0 || !content.some(p => p.type === 'text')) {
          content.push({ type: 'text', text: '.' });
        }
      }
      return { ...m, content };
    });
  }

  async onMessage(ws, evt) {
    await this.autopsy();
    let msg;
    try { msg = JSON.parse(String(evt.data || '')); }
    catch { return this.send(ws, { type: 'err', message: 'bad_json' }); }

    if (msg.type === 'stop') { if (msg.rid && msg.rid === this.rid) this.stop(); return; }
    if (msg.type !== 'begin') return this.send(ws, { type: 'err', message: 'bad_type' });

    const { rid, apiKey, or_body, model, messages, after, provider } = msg;
    let body = or_body || (model && Array.isArray(messages) ? { model, messages, stream: true, ...msg } : null);
    
    if (!rid || !apiKey || !body || !Array.isArray(body.messages) || body.messages.length === 0) return this.send(ws, { type: 'err', message: 'missing_fields' });
    
    // Sanitize messages to prevent 400 errors from strict providers like Moonshot
    body.messages = this.sanitizeMessages(body.messages);

    if (this.phase === 'running' && rid !== this.rid) return this.send(ws, { type: 'err', message: 'busy' });
    
    if (rid === this.rid && this.phase !== 'idle') return this.replay(ws, Number.isFinite(+after) ? +after : -1);

    await this.resetStorage();
    this.rid = rid;
    this.phase = 'running';
    this.controller = new AbortController();
    this.messages = body.messages;
    
    await Promise.all([
      this.state.storage.put('prompt', this.messages),
      this.saveSnapshot()
    ]);
    
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
    } finally {
      if (this.phase === 'running') this.stop();
    }
  }

  async streamOpenAI({ apiKey, body }) {
    const client = new OpenAI({ apiKey });
    const params = { model: body.model, input: this.buildInputForResponses(body.messages || []), temperature: body.temperature, stream: true };
    if (Number.isFinite(+body.max_tokens) && +body.max_tokens > 0) params.max_output_tokens = +body.max_tokens;
    if (Number.isFinite(+body.top_p)) params.top_p = +body.top_p;
    if (body.reasoning?.effort) params.reasoning = { effort: body.reasoning.effort };
    if (body.verbosity) params.text = { verbosity: body.verbosity };
    this.oaStream = await client.responses.stream(params);
    try {
      for await (const event of this.oaStream) {
        if (this.phase !== 'running') break;
        if (event.type.endsWith('.delta') && event.delta) this.queueDelta(event.delta);
      }
    } finally {
      try { this.oaStream?.controller?.abort(); } catch {}
      this.oaStream = null;
    }
  }

  async streamClaude({ apiKey, body }) {
    const client = new Anthropic({ apiKey });
    const system = body.messages
      .filter(m => m.role === 'system')
      .map(m => this.extractTextFromMessage(m))
      .join('\n\n') || body.system;
    const payload = {
      model: body.model,
      messages: body.messages.filter(m => m.role !== 'system').map(m => ({
        role: m.role,
        content: typeof m.content === 'string' ? m.content : (m.content || []).map(p => {
          if (p.type === 'text' && p.text) return { type: 'text', text: p.text };
          if (p.type === 'image_url') {
            const match = String(p.image_url?.url || p.image_url || '').match(/^data:(image\/\w+);base64,(.*)$/);
            if (match) return { type: 'image', source: { type: 'base64', media_type: match[1], data: match[2] } };
          }
        }).filter(Boolean)
      })).filter(m => m.content.length),
      max_tokens: body.max_tokens || 64000,
    };
    if (system) payload.system = system;
    if (Number.isFinite(+body.temperature)) payload.temperature = +body.temperature;
    if (Number.isFinite(+body.top_p)) payload.top_p = +body.top_p;
    if (body.reasoning?.enabled) payload.extended_thinking = { enabled: true, ...(body.reasoning.budget && { max_thinking_tokens: body.reasoning.budget }) };

    const stream = client.messages.stream(payload);
    stream.on('text', text => { if (this.phase === 'running') this.queueDelta(text); });
    await stream.finalMessage();
  }
  
  async streamGoogle({ apiKey, body }) {
    const generationConfig = Object.entries({ temperature: body.temperature, topP: body.top_p, maxOutputTokens: body.max_tokens }).reduce((acc, [k, v]) => (Number.isFinite(+v) && +v >= 0 ? { ...acc, [k]: +v } : acc), {});
    if (body.reasoning) generationConfig.thinkingConfig = { includeThoughts: body.reasoning.exclude !== true, ...(body.reasoning.effort && body.reasoning.effort !== 'default' && { thinkingLevel: body.reasoning.effort }) };
    if (body.response_format?.type?.startsWith('json')) {
      generationConfig.responseMimeType = 'application/json';
      if (body.response_format.json_schema) {
        const translate = s => {
          if (typeof s !== 'object' || s === null) return s;
          const n = Array.isArray(s) ? [] : {};
          for (const k in s) if (Object.hasOwn(s, k)) n[k] = (k === 'type' && typeof s[k] === 'string') ? s[k].toUpperCase() : translate(s[k]);
          return n;
        };
        generationConfig.responseSchema = translate(body.response_format.json_schema.schema || body.response_format.json_schema);
      }
    }
    const model = (body.model ?? '').replace(/:online$/, '');
    const payload = { contents: this.mapToGoogleContents(body.messages), ...(Object.keys(generationConfig).length && { generationConfig }), ...((body.model ?? '').endsWith(':online') && { tools: [{ google_search: {} }] }) };
    const resp = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${model}:streamGenerateContent?alt=sse`, { method: 'POST', headers: { 'Content-Type': 'application/json', 'x-goog-api-key': apiKey }, body: JSON.stringify(payload), signal: this.controller.signal });
    if (!resp.ok) throw new Error(`Google API error: ${resp.status} ${await resp.text()}`);
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '', hasReasoning = false, hasContent = false;
    while (this.phase === 'running') {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      for (const line of buffer.split('\n')) {
        if (!line.startsWith('data: ')) continue;
        try {
          JSON.parse(line.substring(6))?.candidates?.[0]?.content?.parts?.forEach(p => {
            if (p.thought?.thought) {
              this.queueDelta(p.thought.thought);
              hasReasoning = true;
            }
            if (p.text) {
              if (hasReasoning && !hasContent) this.queueDelta('\n');
              this.queueDelta(p.text);
              hasContent = true;
            }
          });
        } catch {}
      }
      buffer = buffer.slice(buffer.lastIndexOf('\n') + 1);
    }
  }

  async streamOpenRouter({ apiKey, body }) {
    const resp = await fetch("https://openrouter.ai/api/v1/chat/completions", {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
        'HTTP-Referer': 'https://sune.chat',
        'X-Title': 'Sune'
      },
      body: JSON.stringify(body),
      signal: this.controller.signal
    });

    if (!resp.ok) throw new Error(`OpenRouter API error: ${resp.status} ${await resp.text()}`);

    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '', hasReasoning = false, hasContent = false;

    while (this.phase === 'running') {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop();

      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        const data = line.substring(6).trim();
        if (data === '[DONE]') break;
        try {
          const j = JSON.parse(data);
          const delta = j.choices?.[0]?.delta;
          if (!delta) continue;

          if (delta.reasoning && body.reasoning?.exclude !== true) {
            this.queueDelta(delta.reasoning);
            hasReasoning = true;
          }
          if (delta.content) {
            if (hasReasoning && !hasContent) this.queueDelta('\n');
            this.queueDelta(delta.content);
            hasContent = true;
          }
          if (delta.images) {
            this.queueDelta('', delta.images);
          }
        } catch {}
      }
    }
  }

  stop() {
    if (this.phase !== 'running') return;
    this.flush(true);
    this.phase = 'done';
    this.error = null;
    try { this.controller?.abort(); } catch {}
    try { this.oaStream?.controller?.abort(); } catch {}
    this.saveSnapshot();
    this.bcast({ type: 'done' });
    this.state.waitUntil(this.stopHeartbeat());
  }

  fail(message) {
    if (this.phase !== 'running') return;
    const errStr = String(message || 'stream_failed');
    this.queueDelta(`\n\nRun failed: ${errStr}`);
    this.flush(true);
    this.phase = 'error';
    this.error = errStr;
    try { this.controller?.abort(); } catch {}
    try { this.oaStream?.controller?.abort(); } catch {}
    this.saveSnapshot();
    this.bcast({ type: 'err', message: this.error });
    this.notify(`Run ${this.rid} failed: ${this.error}`, 3, ['rotating_light']);
    this.state.waitUntil(this.stopHeartbeat());
  }

  async startHeartbeat() {
    if (this.hbActive || this.phase !== 'running') return;
    this.hbActive = true;
    await this.state.storage.setAlarm(Date.now() + HB_INTERVAL_MS).catch(() => {});
  }

  async stopHeartbeat() {
    if (!this.hbActive) return;
    this.hbActive = false;
    const ageSeconds = (this.age * HB_INTERVAL_MS) / 1000;
    this.notify(`Run ${this.rid} ended. Phase: ${this.phase}. Age: ${ageSeconds.toFixed(1)}s.`, 3, ['stop_sign']);
    await this.state.storage.setAlarm(null).catch(() => {});
  }

  async Heart() {
    if (this.phase !== 'running' || !this.hbActive) return this.stopHeartbeat();
    
    ///////////// Debug: To be removed
    this.notify(`Heartbeat for ${this.rid}: age ${this.age}`, 3, ['heartbeat']);
    ///////////// Debug: To be removed

    if (++this.age * HB_INTERVAL_MS >= MAX_RUN_MS) return this.fail(`Run timed out after ${MAX_RUN_MS / 60000} minutes.`);
    await this.state.storage.setAlarm(Date.now() + HB_INTERVAL_MS).catch(() => {});
  }

  async alarm() {
    await this.autopsy();
    await this.Heart();
  }
  
  isMultimodalMessage(m) { return m && Array.isArray(m.content) && m.content.some(p => p?.type && p.type !== 'text' && p.type !== 'input_text'); }

  extractTextFromMessage(m) {
    if (!m) return '';
    if (typeof m.content === 'string') return String(m.content);
    if (!Array.isArray(m.content)) return '';
    return m.content.filter(p => p && ['text', 'input_text'].includes(p.type)).map(p => String(p.type === 'text' ? (p.text ?? p.content ?? '') : (p.text ?? ''))).join('');
  }

  mapContentPartToResponses(part) {
    const type = part?.type || 'text';
    if (['image_url', 'input_image'].includes(type)) return (part?.image_url?.url || part?.image_url) ? { type: 'input_image', image_url: String(part?.image_url?.url || part?.image_url) } : null;
    if (['text', 'input_text'].includes(type)) return { type: 'input_text', text: String(type === 'text' ? (part.text ?? part.content ?? '') : (part.text ?? '')) };
    return { type: 'input_text', text: `[${type}:${part?.file?.filename || 'file'}]` };
  }

  buildInputForResponses(messages) {
    if (!Array.isArray(messages) || messages.length === 0) return '';
    if (!messages.some(m => this.isMultimodalMessage(m))) {
      if (messages.length === 1) return this.extractTextFromMessage(messages[0]);
      return messages.map(m => ({ role: m.role, content: this.extractTextFromMessage(m) }));
    }
    return messages.map(m => ({ role: m.role, content: Array.isArray(m.content) ? m.content.map(p => this.mapContentPartToResponses(p)).filter(Boolean) : [{ type: 'input_text', text: String(m.content || '') }] }));
  }

  mapToGoogleContents(messages) {
    const contents = messages.reduce((acc, m) => {
      const role = m.role === 'assistant' ? 'model' : 'user';
      const msgContent = Array.isArray(m.content) ? m.content : [{ type: 'text', text: String(m.content ?? '') }];
      const parts = msgContent.map(p => {
        if (p.type === 'text') return { text: p.text || '' };
        if (p.type === 'image_url' && p.image_url?.url) {
          const match = p.image_url.url.match(/^data:(image\/\w+);base64,(.*)$/);
          if (match) return { inline_data: { mime_type: match[1], data: match[2] } };
        }
        return null;
      }).filter(Boolean);
      if (!parts.length) return acc;
      if (acc.length > 0 && acc.at(-1).role === role) acc.at(-1).parts.push(...parts);
      else acc.push({ role, parts });
      return acc;
    }, []);
    if (contents.at(-1)?.role !== 'user') contents.pop();
    return contents;
  }
}
