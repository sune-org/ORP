import OpenAI from 'openai';

// --- Constants for stream and state management ---

/** Time-to-live for a completed run's state in storage. (20 minutes) */
const TTL_MS = 20 * 60 * 1000;
/** Interval for batching text deltas before flushing to clients. (800ms) */
const BATCH_MS = 800;
/** Maximum size of the pending text buffer before flushing. (3400 bytes) */
const BATCH_BYTES = 3400;
/** Interval for the durable object heartbeat to keep it active during a run. (3 seconds) */
const HB_INTERVAL_MS = 3000;
/** Maximum allowed duration for a single run before timing out. (8 minutes) */
const MAX_RUN_MS = 8 * 60 * 1000;

/** Standard CORS headers for all responses. */
const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Max-Age': '86400',
};

/**
 * A helper function to wrap a Response with CORS headers.
 * @param {Response} resp The original response.
 * @returns {Response} A new Response with CORS headers applied.
 */
const withCORS = (resp) => {
  const headers = new Headers(resp.headers);
  Object.entries(CORS_HEADERS).forEach(([k, v]) => headers.set(k, v));
  return new Response(resp.body, { ...resp, headers });
};

/**
 * Cloudflare Worker entrypoint.
 * Handles incoming HTTP requests, routing them to the Durable Object.
 */
export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const method = req.method.toUpperCase();

    // Handle CORS preflight requests.
    if (method === 'OPTIONS') return new Response(null, { status: 204, headers: CORS_HEADERS });

    // Enforce an origin whitelist for security.
    if ((h => h !== 'sune.planetrenox.com' && h !== 'sune.chat' && !h.endsWith('.github.io'))(new URL(req.headers.get('Origin') || 'null').hostname)) {
      return withCORS(new Response('Forbidden', { status: 403 }));
    }

    // Route requests for '/ws' to the Durable Object.
    if (url.pathname === '/ws') {
      const isGet = method === 'GET';
      const isWs = req.headers.get('Upgrade') === 'websocket';
      if (!isGet && !isWs) return withCORS(new Response('method not allowed', { status: 405 }));

      // Extract and sanitize a unique identifier for the DO instance.
      const uid = (url.searchParams.get('uid') || '').slice(0, 64).replace(/[^a-zA-Z0-9_-]/g, '');
      if (!uid) return withCORS(new Response('uid is required', { status: 400 }));
      
      // Get a stub for the Durable Object instance associated with the UID.
      const id = env.MY_DURABLE_OBJECT.idFromName(uid);
      const stub = env.MY_DURABLE_OBJECT.get(id);

      // Forward the request to the Durable Object.
      const resp = await stub.fetch(req);
      return isWs ? resp : withCORS(resp);
    }

    return withCORS(new Response('not found', { status: 404 }));
  }
}

/**
 * A Durable Object that manages the state and lifecycle of a single AI stream.
 * It handles WebSocket connections, streams responses from AI providers,
 * and persists state to handle client reconnections or server evictions.
 */
export class MyDurableObject {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.sockets = new Set(); // Stores active WebSocket connections.
    this.reset();
  }

  /** Resets the object to its initial, idle state. */
  reset() {
    this.rid = null;          // Unique ID for the current run.
    this.buffer = [];         // Array of text deltas from the AI.
    this.seq = -1;            // Sequence number for the last delta.
    this.phase = 'idle';      // Current state: 'idle', 'running', 'done', 'error', 'evicted'.
    this.error = null;        // Error message if the run failed.
    this.controller = null;   // AbortController for the ongoing fetch request.
    this.oaStream = null;     // Reference to the OpenAI stream object for abortion.
    this.pending = '';        // Temporary buffer for incoming text deltas before flushing.
    this.flushTimer = null;   // Timer for batching flushes.
    this.lastSavedAt = 0;     // Timestamp of the last snapshot save.
    this.lastFlushedAt = 0;   // Timestamp of the last delta flush.
    this.hbActive = false;    // Flag indicating if the heartbeat is active.
    this.age = 0;             // Counter for heartbeat intervals, used for timeout.
  }

  /**
   * Creates a JSON response with appropriate CORS and cache headers.
   * @param {object} obj The object to serialize into JSON.
   * @param {number} [status=200] The HTTP status code.
   * @returns {Response}
   */
  corsJSON(obj, status = 200) {
    return new Response(JSON.stringify(obj), { status, headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store', ...CORS_HEADERS } });
  }

  /**
   * Safely sends a JSON-stringified object to a WebSocket client.
   * @param {WebSocket} ws The WebSocket client.
   * @param {object} obj The object to send.
   */
  send(ws, obj) { try { ws.send(JSON.stringify(obj)); } catch {} }

  /**
   * Broadcasts a JSON-stringified object to all connected WebSocket clients.
   * @param {object} obj The object to broadcast.
   */
  bcast(obj) { this.sockets.forEach(ws => this.send(ws, obj)); }

  /**
   * Restores the DO's state from storage. Called on first activation or after eviction.
   * This prevents loss of state if the DO is moved or restarted.
   */
  async autopsy() {
    if (this.rid) return; // State is already active in memory.
    const snap = await this.state.storage.get('run').catch(() => null);

    // If no snapshot exists or it's expired, clear storage and do nothing.
    if (!snap || (Date.now() - (snap.savedAt || 0) >= TTL_MS)) {
      if (snap) await this.state.storage.delete('run').catch(() => {});
      return;
    }

    // Restore state from the snapshot.
    this.rid = snap.rid || null;
    this.buffer = Array.isArray(snap.buffer) ? snap.buffer : [];
    this.seq = Number.isFinite(+snap.seq) ? +snap.seq : -1;
    this.age = snap.age || 0;
    this.phase = snap.phase || 'done';
    this.error = snap.error || null;
    this.pending = '';

    // If the DO was evicted mid-run, update the state to reflect this.
    if (this.phase === 'running') {
      this.phase = 'evicted';
      this.error = 'The run was interrupted due to system eviction.';
      this.saveSnapshot();
      await this.stopHeartbeat();
    }
  }

  /** Saves the current run's state to persistent storage. */
  saveSnapshot() {
    this.lastSavedAt = Date.now();
    const snapshot = { rid: this.rid, buffer: this.buffer, seq: this.seq, age: this.age, phase: this.phase, error: this.error, savedAt: this.lastSavedAt };
    return this.state.storage.put('run', snapshot).catch(() => {});
  }

  /**
   * Sends buffered history to a newly connected client.
   * @param {WebSocket} ws The WebSocket client.
   * @param {number} after The sequence number after which to send deltas.
   */
  replay(ws, after) {
    this.buffer.forEach(it => { if (it.seq > after) this.send(ws, { type: 'delta', seq: it.seq, text: it.text }); });
    if (this.phase === 'done') this.send(ws, { type: 'done' });
    else if (['error', 'evicted'].includes(this.phase)) this.send(ws, { type: 'err', message: this.error || 'The run was terminated unexpectedly.' });
  }

  /**
   * Flushes the pending text buffer, sending it to clients and saving it.
   * @param {boolean} [force=false] If true, forces a snapshot save.
   */
  flush(force = false) {
    if (this.flushTimer) { clearTimeout(this.flushTimer); this.flushTimer = null; }
    if (this.pending) {
      this.buffer.push({ seq: ++this.seq, text: this.pending });
      this.bcast({ type: 'delta', seq: this.seq, text: this.pending });
      this.pending = '';
      this.lastFlushedAt = Date.now();
    }
    if (force) this.saveSnapshot();
  }

  /**
   * Queues a text delta to be sent. Manages batching by size and time.
   * @param {string} text The text delta from the AI stream.
   */
  queueDelta(text) {
    if (!text) return;
    this.pending += text;
    if (this.pending.length >= BATCH_BYTES) this.flush(false);
    else if (!this.flushTimer) this.flushTimer = setTimeout(() => this.flush(false), BATCH_MS);
  }

  /**
   * Durable Object's own fetch handler.
   * Manages WebSocket upgrades and GET requests for the current state.
   */
  async fetch(req) {
    if (req.method === 'OPTIONS') return new Response(null, { status: 204, headers: CORS_HEADERS });

    // Handle WebSocket upgrade requests.
    if (req.headers.get('Upgrade') === 'websocket') {
      const [client, server] = Object.values(new WebSocketPair());
      server.accept();
      this.sockets.add(server);
      server.addEventListener('close', () => this.sockets.delete(server));
      server.addEventListener('message', e => this.state.waitUntil(this.onMessage(server, e)));
      return new Response(null, { status: 101, webSocket: client });
    }

    // Handle GET requests to poll the current state.
    if (req.method === 'GET') {
      await this.autopsy(); // Ensure state is loaded.
      const text = this.buffer.map(it => it.text).join('') + this.pending;
      const isTerminal = ['done', 'error', 'evicted'].includes(this.phase);
      const isError = ['error', 'evicted'].includes(this.phase);
      const payload = { rid: this.rid, seq: this.seq, phase: this.phase, done: isTerminal, error: isError ? (this.error || 'The run was terminated unexpectedly.') : null, text };
      return this.corsJSON(payload);
    }
    return this.corsJSON({ error: 'not allowed' }, 405);
  }

  /**
   * Handles incoming messages from a WebSocket client.
   * @param {WebSocket} ws The WebSocket client that sent the message.
   * @param {MessageEvent} evt The message event.
   */
  async onMessage(ws, evt) {
    await this.autopsy();
    let msg;
    try { msg = JSON.parse(String(evt.data || '')); }
    catch { return this.send(ws, { type: 'err', message: 'bad_json' }); }

    // Handle a 'stop' request from the client.
    if (msg.type === 'stop') { if (msg.rid && msg.rid === this.rid) this.stop(); return; }
    if (msg.type !== 'begin') return this.send(ws, { type: 'err', message: 'bad_type' });

    const { rid, apiKey, or_body, model, messages, after, provider } = msg;
    const body = or_body || (model && Array.isArray(messages) ? { model, messages, stream: true, ...msg } : null);
    
    // Validate required fields for starting a new run.
    if (!rid || !apiKey || !body || !Array.isArray(body.messages) || body.messages.length === 0) return this.send(ws, { type: 'err', message: 'missing_fields' });
    
    // Prevent a new run if one is already in progress with a different ID.
    if (this.phase === 'running' && rid !== this.rid) return this.send(ws, { type: 'err', message: 'busy' });
    
    // If client reconnects to an existing run, just replay history.
    if (rid === this.rid && this.phase !== 'idle') return this.replay(ws, Number.isFinite(+after) ? +after : -1);

    // Initialize state for a new run.
    this.reset();
    this.rid = rid;
    this.phase = 'running';
    this.controller = new AbortController();
    await this.saveSnapshot();
    
    // Start background tasks for the run.
    this.state.waitUntil(this.startHeartbeat());
    this.state.waitUntil(this.stream({ apiKey, body, provider: provider || 'openrouter' }));
  }

  /**
   * Orchestrates the AI stream, selecting the correct provider implementation.
   * @param {{apiKey: string, body: object, provider: string}} params
   */
  async stream({ apiKey, body, provider }) {
    try {
      const providerMap = { openai: this.streamOpenAI, google: this.streamGoogle };
      await (providerMap[provider] || this.streamOpenRouter).call(this, { apiKey, body });
    } catch (e) {
      if (this.phase === 'running') {
        const msg = String(e?.message || 'stream_failed');
        // Don't treat explicit aborts as failures.
        if (!((e && e.name === 'AbortError') || /abort/i.test(msg))) this.fail(msg);
      }
    } finally {
      // Ensure the run is properly stopped if it hasn't been already.
      if (this.phase === 'running') this.stop();
    }
  }

  /**
   * Handles streaming from OpenAI-compatible "Responses" API (e.g., Sune).
   */
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
        if (this.phase !== 'running') break; // Stop if the run was cancelled.
        if (event.type.endsWith('.delta') && event.delta) this.queueDelta(event.delta);
      }
    } finally {
      try { this.oaStream?.controller?.abort(); } catch {}
      this.oaStream = null;
    }
  }
  
  /**
   * Handles streaming from Google's Gemini API.
   */
  async streamGoogle({ apiKey, body }) {
    const generationConfig = Object.entries({ temperature: body.temperature, topP: body.top_p, maxOutputTokens: body.max_tokens }).reduce((acc, [k, v]) => (Number.isFinite(+v) && +v >= 0 ? { ...acc, [k]: +v } : acc), {});
    if (body.reasoning) generationConfig.thinkingConfig = { includeThoughts: body.reasoning.exclude !== true };
    if (body.response_format?.type?.startsWith('json')) {
      generationConfig.responseMimeType = 'application/json';
      if (body.response_format.json_schema) {
        // Recursively transform 'type' values to uppercase for Google's API.
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
    let buffer = '';
    while (this.phase === 'running') {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      // Process Server-Sent Events (SSE).
      for (const line of buffer.split('\n')) {
        if (!line.startsWith('data: ')) continue;
        try { JSON.parse(line.substring(6))?.candidates?.[0]?.content?.parts?.forEach(p => p.text && this.queueDelta(p.text)); } catch {}
      }
      buffer = buffer.slice(buffer.lastIndexOf('\n') + 1);
    }
  }

  /**
   * Handles streaming from OpenRouter's API.
   */
  async streamOpenRouter({ apiKey, body }) {
    const client = new OpenAI({ apiKey, baseURL: 'https://openrouter.ai/api/v1' });
    const stream = await client.chat.completions.create({ ...body, stream: true }, { signal: this.controller.signal });
    for await (const chunk of stream) {
      if (this.phase !== 'running') break; // Stop if the run was cancelled.
      const delta = chunk?.choices?.[0]?.delta;
      if (delta?.reasoning && body.reasoning?.exclude !== true) this.queueDelta(delta.reasoning);
      if (delta?.content) this.queueDelta(delta.content);
    }
  }

  /**
   * Gracefully stops the current run.
   */
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

  /**
   * Stops the current run due to an error.
   * @param {string} message The error message.
   */
  fail(message) {
    if (this.phase !== 'running') return;
    this.flush(true);
    this.phase = 'error';
    this.error = String(message || 'stream_failed');
    try { this.controller?.abort(); } catch {}
    try { this.oaStream?.controller?.abort(); } catch {}
    this.saveSnapshot();
    this.bcast({ type: 'err', message: this.error });
    this.state.waitUntil(this.stopHeartbeat());
  }

  /**
   * Starts the heartbeat mechanism to keep the DO alive during a run.
   * It schedules an alarm which will trigger the `alarm()` handler.
   */
  async startHeartbeat() {
    if (this.hbActive || this.phase !== 'running') return;
    this.hbActive = true;
    await this.state.storage.setAlarm(Date.now() + HB_INTERVAL_MS).catch(() => {});
  }

  /** Stops the heartbeat mechanism. */
  async stopHeartbeat() {
    this.hbActive = false;
    // A null alarm time deletes the alarm.
    await this.state.storage.setAlarm(null).catch(() => {});
  }

  /**
   * The core logic of the heartbeat. Checks for timeout and schedules the next alarm.
   */
  async Heart() {
    if (this.phase !== 'running' || !this.hbActive) return this.stopHeartbeat();
    // Check if the run has exceeded the maximum duration.
    if (++this.age * HB_INTERVAL_MS >= MAX_RUN_MS) return this.fail('Run timed out after 15 minutes.');
    await this.state.storage.setAlarm(Date.now() + HB_INTERVAL_MS).catch(() => {});
  }

  /**
   * The alarm handler, automatically called by the runtime when an alarm is set.
   */
  async alarm() {
    await this.autopsy(); // Ensure state is loaded, in case of eviction.
    await this.Heart();
  }
  
  // --- Message Format Transformation Helpers ---

  /** Checks if a message contains non-text content parts. */
  isMultimodalMessage(m) { return m && Array.isArray(m.content) && m.content.some(p => p?.type && p.type !== 'text' && p.type !== 'input_text'); }

  /** Extracts all plain text from a message object, ignoring other content types. */
  extractTextFromMessage(m) {
    if (!m) return '';
    if (typeof m.content === 'string') return String(m.content);
    if (!Array.isArray(m.content)) return '';
    return m.content.filter(p => p && ['text', 'input_text'].includes(p.type)).map(p => String(p.type === 'text' ? (p.text ?? p.content ?? '') : (p.text ?? ''))).join('');
  }

  /** Maps a single content part from a generic format to the Sune "Responses" API format. */
  mapContentPartToResponses(part) {
    const type = part?.type || 'text';
    if (['image_url', 'input_image'].includes(type)) return (part?.image_url?.url || part?.image_url) ? { type: 'input_image', image_url: String(part?.image_url?.url || part?.image_url) } : null;
    if (['text', 'input_text'].includes(type)) return { type: 'input_text', text: String(type === 'text' ? (part.text ?? part.content ?? '') : (part.text ?? '')) };
    // Represent other file types with a placeholder text.
    return { type: 'input_text', text: `[${type}:${part?.file?.filename || 'file'}]` };
  }

  /** Builds the `input` field for the Sune "Responses" API from a list of messages. */
  buildInputForResponses(messages) {
    if (!Array.isArray(messages) || messages.length === 0) return '';
    // If there's no multimodal content, we can use a simpler format.
    if (!messages.some(m => this.isMultimodalMessage(m))) {
      if (messages.length === 1) return this.extractTextFromMessage(messages[0]);
      return messages.map(m => ({ role: m.role, content: this.extractTextFromMessage(m) }));
    }
    // Handle complex multimodal inputs.
    return messages.map(m => ({ role: m.role, content: Array.isArray(m.content) ? m.content.map(p => this.mapContentPartToResponses(p)).filter(Boolean) : [{ type: 'input_text', text: String(m.content || '') }] }));
  }

  /** Transforms a generic message list into the format required by Google's Gemini API. */
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
      // Google's API requires alternating user/model roles. Merge consecutive messages with the same role.
      if (acc.length > 0 && acc.at(-1).role === role) acc.at(-1).parts.push(...parts);
      else acc.push({ role, parts });
      return acc;
    }, []);
    // The last message must be from the user.
    if (contents.at(-1)?.role !== 'user') contents.pop();
    return contents;
  }
}
