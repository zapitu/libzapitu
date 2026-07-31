# Worker Mode Technical Documentation

## Change Reference

- **Base commit:** `3080d402dbd204619577a83cc3a12c1ed24c627e`
- **Documented HEAD:** `0d365a9` (branch `rf`) + post-doc fixes for `wsocket.user` proxy
- **Scope:** All changes introduced between the base commit and HEAD, generated exclusively from the repository diff.

## Objective

Introduce an optional **worker-thread encapsulation mode** for `makeWASocket` so that the WhatsApp Web socket engine can run inside Node.js `worker_threads` instead of the main thread. The main goals are:

1. Off-load the CPU-intensive and event-heavy socket lifecycle from the main event loop.
2. Allow multiple concurrent sockets to run inside a shared pool of worker threads.
3. Keep the public API as close as possible to the existing direct-import API so that callers can switch between direct mode and worker mode by changing only the import path.
4. Support async configuration callbacks (`shouldIgnoreJid`, `shouldSyncHistoryMessage`) that were previously synchronous.

## Summary of Changed Files

| File                          | What changed                                                                                                                          |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `src/worker/child.ts`         | New worker-thread entry point. Runs the real `makeWASocket`, bridges RPC calls, forwards events, manages per-socket state, and sends `socket-info` synchronously on `connection.open`. |
| `src/worker/proxy.ts`         | New parent-thread pool manager. Spawns/holds `Worker` instances, distributes sockets, proxies method calls, exposes pool control, and syncs `user` properties from `creds.update` events. |
| `src/worker/index.ts`         | New public subpath export `libzapitu-rf/worker` (or `../src/worker` for source). Re-exports core types/utils and the worker factory.  |
| `package.json`                | Added `./worker` export, `typesVersions`, bumped version to `1.0.0-alpha.20`, added `node-cache` and `qrcode-terminal` dependencies.  |
| `Example/example.ts`          | Added `--worker` CLI flag and dynamic import of `../src/worker`; QR printing, reconnect delay, and worker-pool logging.               |
| `Example/worker-test.ts`      | New dedicated example that connects via worker mode, sends a message, and waits for acks and replies.                                 |
| `src/Types/Socket.ts`         | `shouldSyncHistoryMessage` and `shouldIgnoreJid` now accept async return values.                                                      |
| `src/Socket/chats.ts`         | `shouldSyncHistoryMessage` call is awaited.                                                                                           |
| `src/Socket/messages-recv.ts` | `shouldIgnoreJid` calls are awaited in notification/message/presence/receipt handlers.                                                |
| `src/Utils/logger.ts`         | Default log level now reads from `BAILEYS_LOG_LEVEL` environment variable (fallback `info`).                                          |
| `CHANGELOG.md`                | Auto-generated entries for `v1.0.0-alpha.17` through `v1.0.0-alpha.20`.                                                               |

---

## 1. Worker Architecture

### 1.1. Public API

```ts
import makeWASocket, { getStats, resizePool, drainPool } from 'libzapitu-rf/worker'

const sock = makeWASocket({
	/* same config as direct mode */
})

sock.ev.on('messages.upsert', handler)
await sock.sendMessage(jid, { text: 'hello' })

console.log(sock.pool) // { size, active, idle, totalSockets }

await resizePool(4)
await drainPool()
```

### 1.2. Runtime Components

- **Parent thread** (`src/worker/proxy.ts`): owns `Worker` instances, maintains `WorkerSlot`s, and returns a `Proxy` object that behaves like a normal `WASocket`.
- **Worker thread** (`src/worker/child.ts`): runs inside `worker_threads`, receives an `init` message, creates the real `makeWASocket`, and forwards events / RPC results back to the parent.

### 1.3. Message Protocol

| Message type      | Direction      | Purpose                                                                                        |
| ----------------- | -------------- | ---------------------------------------------------------------------------------------------- |
| `init`            | parent → child | Create a socket with a given `socketId` and serializable config.                               |
| `call`            | parent → child | RPC call to a socket method or nested path (`ev.flush`, `ws.close`, etc.).                     |
| `result`          | child → parent | RPC response or property read result.                                                          |
| `event`           | child → parent | Aggregated Baileys event map (forwarded as `event` and split into individual typed events).    |
| `ws-event`        | child → parent | Raw WebSocket emitter events (`ws.on(...)`).                                                   |
| `socket-info`     | child → parent | Sends `user` and `authState` snapshots so the proxy can expose `sock.user` / `sock.authState`. |
| `callback-call`   | child → parent | Worker asks the parent to execute a config callback (`getMessage`, `shouldIgnoreJid`, etc.).   |
| `callback-result` | parent → child | Response for a callback call.                                                                  |

---

## 2. Parent-Side Pool Manager (`src/worker/proxy.ts`)

### 2.1. Pool Sizing

- Default pool size equals `cpus().length`.
- New sockets are placed on the least-loaded worker.
- If every worker already has at least one socket and the pool has not reached `maxWorkers`, a new worker is spawned.
- `resizePool(n)` changes the upper bound and terminates idle excess workers.
- `drainPool()` waits for all sockets to close and then terminates every worker.

### 2.2. Config Serialization

Non-serializable values are stripped or replaced with sentinels before `postMessage`:

- Functions → `'__proxy_fn__'`.
- Config callbacks (`getMessage`, `shouldIgnoreJid`, `patchMessageBeforeSending`, `cachedGroupMetadata`, `shouldSyncHistoryMessage`) → `'__proxy_callback__'`; their real implementations are stored in `slot.callbacks`.
- `auth.keys` → `'__proxy_keystore__'`; the real `SignalKeyStore` is stored in `slot.keystores`.
- `logger` → `'__proxy_logger__'`; the child recreates a real logger.
- `makeSignalRepository` → `'__worker_signal_repository__'`; the child recreates the libsignal repository locally.
- Worker-local caches (`msgRetryCounterCache`, `mediaCache`, `userDevicesCache`, `callOfferCache`, `placeholderResendCache`) → `'__worker_cache__'`; fresh `NodeCache` instances are created in the worker.
- `agent` and `fetchAgent` → `undefined`; the worker creates its own HTTP agents.

### 2.3. Proxy Behavior

- `ev` is a `Proxy` around a local `EventEmitter`.
  - EventEmitter methods (`on`, `off`, `once`, etc.) operate locally.
  - `process()` is implemented locally by listening to the aggregated `'event'` emitted by the worker bridge.
  - Other `ev` methods (`buffer`, `flush`, `createBufferedFunction`, `isBuffering`) are forwarded via RPC (`ev.<method>`).
- `ws` is a `Proxy` around a local `EventEmitter`.
  - `ws.close()` is forwarded via RPC.
  - All other `EventEmitter` methods work locally.
- All other property accesses are resolved as RPC calls unless they are:
  - `ev` or `ws` (proxied),
  - `pool` (returns live `getStats()`),
  - `end` / `logout` (call RPC and then run cleanup),
  - a value already cached in the local `props` store (e.g. `user`, `authState`).
- `set` traps store values locally in `props`.
- **`user` property sync:** When a `creds.update` event arrives from the worker, the proxy merges `me` into `localProps.user` (in addition to syncing `config.auth.creds`). This keeps `wsocket.user` and all its nested properties (`id`, `lid`, `name`, `verifiedName`, `imgUrl`, `status`, `notify`) in sync with the worker's real `authState.creds.me` across the socket lifecycle.

### 2.4. Lifecycle & Cleanup

- `cleanupSocket(slot, socketId, log)` removes all per-socket state (emitters, callbacks, keystore, logger, pending RPCs) and decrements the worker load.
- Pending RPCs for a closed socket are rejected with `Socket {socketId} closed` so they do not hang.
- When a worker emits a `connection.update` with `connection: 'close'`, the proxy performs cleanup.

### 2.5. Error Revival

`revivePostMessage()` converts `__error__` sentinels back into real `Error` instances so that callers receive proper error objects even though the data crossed the worker boundary.

### 2.6. Stream Serialization for `sendMessage`

The `sendMessage` method accepts `WAMediaUpload` values that can contain `Readable` streams (`{ stream: Readable }`). Since `worker.postMessage()` uses the structured clone algorithm, which cannot serialize streams, the proxy must convert them before crossing the RPC boundary.

#### Strategy

```
{ stream: Readable }
        │
        ▼
  tryGetStreamSize()
        │
   ┌────┴────┐
   │ known   │ unknown
   ▼         ▼
 ≤50MB?   temp file
  │  │
  ▼  ▼
buffer temp file
  │      │
  ▼      ▼
Buffer  { url: "/tmp/zapitu-proxy-upload-..." }
```

| Scenario | Action | Rationale |
|---|---|---|
| Size known, ≤ 50 MB | Read stream into `Buffer` | Fast, no disk I/O, fits comfortably in memory |
| Size known, > 50 MB | Write to temp file, pass `{ url: filePath }` | Avoids memory pressure from large uploads |
| Size unknown | Write to temp file (safe default) | Cannot risk buffering an unbounded stream |

#### Size Detection

`tryGetStreamSize()` checks `headers['content-length']` on HTTP-like streams (e.g., axios responses). For `fs.ReadStream` or generic `Readable` instances, the size is treated as unknown and the temp-file path is taken.

#### Conversion Functions

| Function | Purpose |
|---|---|
| `streamToBuffer(stream, maxBytes)` | Reads stream into a `Buffer`, enforcing a hard byte limit. Destroys the stream and throws if exceeded. |
| `streamToTempFile(stream)` | Pipes stream to `$TMPDIR/zapitu-proxy-upload-{ts}-{random}`. Cleans up partial file on error. |
| `serializeStream(stream)` | Decision logic: chooses buffer or temp file based on known size. |
| `serializeStreamArgs(args)` | Recursively walks `sendMessage` arguments, finds `{ stream: Readable }` shapes, converts them. Returns `{ processed, cleanup }`. |

#### Worker-Side Compatibility

- **Buffer path:** The raw `Buffer` is passed directly. The worker's `getStream()` handles `Buffer.isBuffer(item)` natively.
- **Temp file path:** The stream is replaced with `{ url: filePath }`. The worker's `getStream()` treats it as a local file via `createReadStream(item.url)`.

#### Cleanup

Temp files are deleted in a `finally` block after the RPC call completes (success or failure), using `Promise.allSettled` so one failed unlink does not block others.

#### Proxy Hook

The `sendMessage` handler in the socket proxy is special-cased:

```ts
if (prop === 'sendMessage') {
    return async (...args: unknown[]) => {
        const { processed, cleanup } = await serializeStreamArgs(args)
        try {
            return await rpcCall('sendMessage', processed)
        } finally {
            await cleanup()
        }
    }
}
```

No other methods require this treatment — `WAMediaUpload` only flows through `sendMessage`.

---

## 3. Worker-Child Bridge (`src/worker/child.ts`)

### 3.1. Initialization

- Rejects execution if not inside a worker thread (`isMainThread` or missing `parentPort`).
- Maintains a `Map<number, SocketEntry>` keyed by `socketId` to support multiple sockets per worker.
- On `init`, the child:
  1. Revives `Uint8Array` values into `Buffer`.
  2. Replaces `__proxy_logger__` with a real logger child.
  3. Replaces callback sentinels with forwarding stubs.
  4. Replaces `__worker_signal_repository__` with `makeLibSignalRepository`.
  5. Replaces `__proxy_keystore__` with a stub that forwards key-store operations to the parent.
  6. Replaces cache sentinels with fresh `NodeCache({ useClones: false })` instances.
  7. Creates the real `makeWASocket`.

### 3.2. Event Forwarding

- All Baileys events from `sock.ev` are forwarded to the parent as `event` messages.
- If a message cannot be cloned, the child falls back to `sanitizeForPostMessage()` which converts `Error` objects to plain `__error__` sentinels and replaces functions with `'__fn__'`.
- WebSocket events are forwarded by monkey-patching `sock.ws.emit` so that `ws.on(...)` works on the proxy side.

### 3.3. RPC Handling

- `call` messages support dot-notation paths (e.g. `groupMetadata`, `ev.flush`) and resolve the correct `this` context.
- If the target is a function, it is invoked with `await` and the result is sent back.
- If the target is a property, its value is returned.
- Errors are caught and sent as `{ error: message }`.

### 3.4. Callback Handling

- For each proxied callback key, the child posts a `callback-call` message to the parent and waits for a `callback-result`.
- Keystore operations are dispatched as `keystore.<method>` callback keys.
- The child sends `socket-info` messages immediately on socket creation and synchronously inside the `connection.update` listener when `connection === 'open'`. The synchronous call (no `setTimeout`) ensures the `socket-info` message is posted to the parent **before** the `connection.update` event is forwarded, so `wsocket.user` is already populated when the parent's connection handler runs.

### 3.5. Socket Cleanup

- When `connection.update` reports `connection: 'close'`, the worker removes the socket entry from its map.
- This allows the same worker thread to be reused for future sockets with different `socketId`s.

---

## 4. Async Callback Support

### 4.1. Motivation

Because callbacks now execute on the parent thread while the socket runs in the worker, the bridge must be asynchronous. It was therefore necessary to allow `shouldIgnoreJid` and `shouldSyncHistoryMessage` to return `Promise<boolean>` in addition to plain booleans.

### 4.2. Type Changes

- `src/Types/Socket.ts`:
  - `shouldSyncHistoryMessage: (msg) => boolean | Promise<boolean>`
  - `shouldIgnoreJid: (jid) => boolean | undefined | Promise<boolean | undefined>`

### 4.3. Call-Site Updates

- `src/Socket/chats.ts`: `await shouldSyncHistoryMessage(historyMsg)`.
- `src/Socket/messages-recv.ts`: `await shouldIgnoreJid(...)` in:
  - `handleReceipt`
  - `handleNotification`
  - `handleMessage`
  - `handlePresenceUpdate`

---

## 5. Example Changes

### 5.1. `Example/example.ts`

- Imports `makeWASocketDirect` from `../src` (direct mode).
- Dynamically imports `makeWASocketWorker` from `../src/worker` when `--worker` is passed.
- Added `qrcode-terminal` QR printing for both modes.
- Added a 2-second reconnect delay on non-logged-out disconnects.
- `sendWAMBuffer` example only runs in direct mode.
- Worker mode exposes `sock.pool` for debugging.

### 5.2. `Example/worker-test.ts`

- New standalone example.
- Usage: `npx ts-node Example/worker-test.ts <target_number> [--use-pairing-code]`.
- Connects via `../src/worker`, sends a text message to the target JID after 60 seconds, and logs message receipts (`server`, `delivery`, `read`, `played`) plus replies.

---

## 6. Logging Improvements

### 6.1. `src/Utils/logger.ts`

- Default logger now respects the `BAILEYS_LOG_LEVEL` environment variable, falling back to `info`.
- This makes it possible to enable `trace` logs in worker children without changing code.

### 6.2. Worker Child Logging

- Worker children log socket creation, RPC calls, event forwarding, callback handling, and errors through a real logger derived from the parent config.
- Errors are sanitized with `sanitizeForPostMessage` before crossing the worker boundary.

---

## 7. Package Configuration

### 7.1. Exports

```json
"exports": {
  ".": { "types": "./lib/index.d.ts", "default": "./lib/index.js" },
  "./worker": { "types": "./lib/worker/index.d.ts", "default": "./lib/worker/index.js" }
},
"typesVersions": {
  "*": {
    "worker": ["./lib/worker/index.d.ts"]
  }
}
```

### 7.2. Published Files

- `files` now includes `lib/worker/*` so the compiled worker files are included in the npm package.

### 7.3. New Dependencies

- `node-cache` (dev/runtime): used for worker-local cache stores.
- `qrcode-terminal` (dev): used in examples to print QR codes in the terminal.

---

## 8. Known Limitations / Design Notes

1. **Callbacks execute on the parent thread.** Heavy synchronous work inside `shouldIgnoreJid` or `getMessage` can still block the parent event loop, though the socket engine itself no longer runs there.
2. **Function config values other than the known callback keys** are stripped and become no-ops in the worker.
3. **HTTP agents (`agent`, `fetchAgent`) are dropped.** The worker creates its own.
4. **WebSocket close / socket end / logout are cleanup-aware.** Calling them after the socket is already cleaned up is a no-op rather than throwing.
5. **Pool stats are live snapshots.** `sock.pool` evaluates `getStats()` on every read.
6. **The `cleanupSocket` function contains dead code after the final `return` statement.** The post-return idle-worker termination block is currently unreachable in the existing implementation.
7. **Stream-based media uploads are converted at the proxy boundary.** `Readable` streams passed to `sendMessage` are eagerly consumed into a `Buffer` (≤ 50 MB) or a temp file (> 50 MB / unknown size) before crossing the RPC boundary. This means the stream is fully read on the parent thread before the worker begins processing — acceptable because the worker's `encryptedStream()` reads the entire file anyway for encryption.

---

## 9. Version History Covered

| Version           | Commit    | Summary                                                            |
| ----------------- | --------- | ------------------------------------------------------------------ |
| `v1.0.0-alpha.17` | `2907550` | Worker thread entry point for `makeWASocket`.                      |
| `v1.0.0-alpha.17` | `50623f3` | Support multiple concurrent sockets per worker thread.             |
| `v1.0.0-alpha.18` | `46ecb1a` | Add `typesVersions` for worker subpath.                            |
| `v1.0.0-alpha.19` | `e75b231` | Add logging to worker child and proxy.                             |
| `v1.0.0-alpha.20` | `706440d` | Add worker test example and sanitize child proxy errors.           |
| post-alpha.20     | `657c098` | Add worker mode support to `example.ts`.                           |
| post-alpha.20     | `ecedf40` | Add message sending and ack handling to `worker-test`.             |
| post-alpha.20     | `0d365a9` | Make `shouldIgnoreJid` and `shouldSyncHistoryMessage` async-aware. |
| post-alpha.20     | (HEAD)    | Fix `wsocket.user` proxy: send `socket-info` synchronously before `connection.update`; sync `creds.update.me` to `localProps.user`. |
| post-alpha.20     | (HEAD)    | Add hybrid stream serialization for `sendMessage` RPC: buffer ≤ 50 MB, temp file for larger/unknown streams. |
