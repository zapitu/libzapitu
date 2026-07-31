/**
 * Worker proxy — parent-side pool manager.
 *
 * Maintains a pool of worker_threads children, each running a real
 * makeWASocket.  The pool automatically distributes new socket requests
 * across workers and provides lifecycle control.
 *
 * Usage (encapsulated mode):
 *   import makeWASocket from 'libzapitu-rf/worker'
 *   const sock = makeWASocket({ ... })
 *   sock.ev.on('messages.upsert', ...)
 *   await sock.sendMessage(...)
 *
 * Pool control (available on the returned socket):
 *   sock.pool — { size, active, idle, totalSockets }
 *
 * Global pool control:
 *   import { resizePool, drainPool, getStats } from 'libzapitu-rf/worker'
 */
import { Worker } from 'worker_threads'
import { EventEmitter } from 'events'
import { cpus } from 'os'
import { existsSync, promises as fsPromises } from 'fs'
import { createWriteStream } from 'fs'
import { join } from 'path'
import { Readable, pipeline } from 'stream'
import { promisify } from 'util'
import { tmpdir } from 'os'
import Long from 'long'
import type { UserFacingSocketConfig } from '../Types'
import type { ILogger } from '../Utils/logger'

const pipelineAsync = promisify(pipeline)

// ---------------------------------------------------------------------------
// Stream serialization for RPC boundary
// ---------------------------------------------------------------------------

/** Maximum bytes to buffer in memory before falling back to a temp file. */
const STREAM_BUFFER_MAX_BYTES = 50 * 1024 * 1024 // 50 MB

/**
 * Result of converting a Readable stream into a postMessage-safe form.
 *
 * - `buffer`: the stream was small enough (or size was known and ≤ max) and
 *   was fully read into a Buffer.
 * - `tempFilePath`: the stream was too large or its size was unknown; data
 *   was written to a temporary file. The path is passed to the worker, which
 *   reads it via `createReadStream`.
 */
type SerializedStream =
	| { buffer: Buffer }
	| { tempFilePath: string }

/**
 * Read a Readable stream into a Buffer, enforcing a maximum size.
 * If the stream exceeds `maxBytes`, it is destroyed and an error is thrown.
 */
async function streamToBuffer(stream: Readable, maxBytes: number): Promise<Buffer> {
	const chunks: Buffer[] = []
	let totalBytes = 0

	for await (const chunk of stream) {
		const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)
		totalBytes += buf.length
		if (totalBytes > maxBytes) {
			stream.destroy()
			throw new Error(`Stream exceeds maximum buffer size of ${maxBytes} bytes`)
		}
		chunks.push(buf)
	}

	return Buffer.concat(chunks)
}

/**
 * Write a Readable stream to a temporary file.
 * Returns the absolute path to the temp file.
 */
async function streamToTempFile(stream: Readable): Promise<string> {
	const tmpPath = join(tmpdir(), `zapitu-proxy-upload-${Date.now()}-${Math.random().toString(36).slice(2)}`)
	const writeStream = createWriteStream(tmpPath)

	try {
		await pipelineAsync(stream, writeStream)
		return tmpPath
	} catch (err) {
		// Clean up partial file on error
		try {
			await fsPromises.unlink(tmpPath)
		} catch {
			/* best-effort */
		}
		throw err
	}
}

/**
 * Attempt to determine the byte size of a stream from its known properties.
 * Returns `undefined` if the size cannot be determined without consuming.
 */
function tryGetStreamSize(stream: Readable): number | undefined {
	// fs.ReadStream exposes bytesRead + a path we could stat, but bytesRead
	// is only populated after reading. Check for content-length on HTTP-like
	// streams (e.g. axios responses).
	const headers = (stream as any)?.headers as Record<string, string> | undefined
	if (headers?.['content-length']) {
		const len = parseInt(headers['content-length'], 10)
		if (!isNaN(len)) return len
	}

	// readableLength is a Node.js internal that gives buffered bytes, not total.
	// Not useful here.

	return undefined
}

/**
 * Convert a Readable stream into a postMessage-safe representation.
 *
 * Strategy:
 * 1. If the stream size is known and ≤ 50 MB → buffer in memory.
 * 2. If the stream size is known and > 50 MB → write to temp file.
 * 3. If the stream size is unknown → write to temp file (safe default).
 *
 * The caller is responsible for cleaning up temp files after the RPC call
 * completes (success or failure).
 */
async function serializeStream(stream: Readable): Promise<SerializedStream> {
	const knownSize = tryGetStreamSize(stream)

	if (knownSize !== undefined && knownSize <= STREAM_BUFFER_MAX_BYTES) {
		const buffer = await streamToBuffer(stream, STREAM_BUFFER_MAX_BYTES)
		return { buffer }
	}

	// Unknown size or known to be large → temp file
	const tempFilePath = await streamToTempFile(stream)
	return { tempFilePath }
}

/**
 * Recursively walk an argument tree and convert any `{ stream: Readable }`
 * objects into postMessage-safe representations.
 *
 * After this function, the args are safe to pass to `worker.postMessage()`.
 * Returns a cleanup function that should be called after the RPC completes
 * to remove any temp files that were created.
 */
async function serializeStreamArgs(
	args: unknown[]
): Promise<{ processed: unknown[]; cleanup: () => Promise<void> }> {
	const tempFiles: string[] = []

	const walk = async (value: unknown): Promise<unknown> => {
		if (value === null || value === undefined) return value
		if (typeof value !== 'object') return value

		// Detect { stream: Readable } — the WAMediaPayloadStream shape
		if ('stream' in (value as any) && (value as any).stream instanceof Readable) {
			const stream = (value as any).stream as Readable
			const result = await serializeStream(stream)
			if ('tempFilePath' in result) {
				tempFiles.push(result.tempFilePath)
				// Replace with { url: filePath } so the worker's getStream()
				// treats it as a local file path (WAMediaPayloadURL shape).
				return { url: result.tempFilePath }
			}
			// Buffer — pass directly; the worker's getStream() handles Buffer natively
			return result.buffer
		}

		if (Array.isArray(value)) {
			return Promise.all(value.map(v => walk(v)))
		}

		if (Buffer.isBuffer(value) || value instanceof Uint8Array || value instanceof ArrayBuffer) {
			return value
		}

		const result: Record<string, unknown> = {}
		for (const [key, val] of Object.entries(value as Record<string, unknown>)) {
			result[key] = await walk(val)
		}
		return result
	}

	const processed = await Promise.all(args.map(a => walk(a)))

	const cleanup = async () => {
		await Promise.allSettled(tempFiles.map(p => fsPromises.unlink(p).catch(() => {})))
	}

	return { processed, cleanup }
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------
const CALLBACK_CONFIG_KEYS = [
	'getMessage',
	'shouldIgnoreJid',
	'patchMessageBeforeSending',
	'cachedGroupMetadata',
	'shouldSyncHistoryMessage'
] as const

type CallbackKey = (typeof CALLBACK_CONFIG_KEYS)[number]

/**
 * Cache stores that live inside the worker thread. They cannot be safely
 * cloned via postMessage because their methods live on the prototype, so we
 * replace them with a sentinel and recreate a fresh NodeCache in the worker.
 */
const CACHE_CONFIG_KEYS = [
	'msgRetryCounterCache',
	'mediaCache',
	'userDevicesCache',
	'callOfferCache',
	'placeholderResendCache'
] as const

type CacheKey = (typeof CACHE_CONFIG_KEYS)[number]
const CACHE_SENTINEL = '__worker_cache__'

interface WorkerSlot {
	worker: Worker
	/** Number of active sockets on this worker */
	load: number
	/** Per-socket event emitters keyed by a unique socketId */
	emitters: Map<number, EventEmitter>
	/** Per-socket raw WS event emitters */
	wsEmitters: Map<number, EventEmitter>
	/** Per-socket local property stores (for wsocket.id, wsocket.user, etc.) */
	props: Map<number, Record<string, unknown>>
	/** Per-socket callback stores */
	callbacks: Map<number, Partial<Record<CallbackKey, Function>>>
	/** Per-socket real auth.keys (SignalKeyStore) — proxied separately */
	keystores: Map<number, any>
	/** Per-socket loggers (child loggers from the user's config) */
	loggers: Map<number, ILogger>
	/** Per-socket original configs (used to sync creds.update back to parent) */
	configs: Map<number, UserFacingSocketConfig>
	/** Pending RPC calls keyed by request id */
	pending: Map<number, { socketId: number; resolve: (v: any) => void; reject: (e: Error) => void }>
}

interface PoolStats {
	/** Total workers in the pool */
	size: number
	/** Workers currently handling at least one socket */
	active: number
	/** Workers with zero sockets */
	idle: number
	/** Total sockets across all workers */
	totalSockets: number
}

// ---------------------------------------------------------------------------
// Globals
// ---------------------------------------------------------------------------
let _reqId = 0
const nextReqId = (): number => ++_reqId
let _socketId = 0
const nextSocketId = (): number => ++_socketId

// Resolve the worker script path. When running via ts-node, __dirname
// points to src/worker/ and we need to spawn the .ts file with ts-node.
// In production (compiled JS), __dirname points to lib/worker/ and we
// use the .js file directly.
const _childJsPath = join(__dirname, 'child.js')
const _childTsPath = join(__dirname, 'child.ts')
const _isTsNode = existsSync(_childTsPath) && !existsSync(_childJsPath)

const workerScript = _isTsNode ? _childTsPath : _childJsPath
const workerExecArgv = _isTsNode ? ['-r', 'ts-node/register'] : undefined

/** Active worker slots */
const slots: WorkerSlot[] = []

/** Maximum workers allowed (default: CPU count) */
let maxWorkers: number = cpus().length

/** No-op logger used when config.logger is not provided */
function createNoopLogger(): ILogger {
	const noop = () => {}
	return {
		level: 'silent',
		child: () => createNoopLogger(),
		trace: noop,
		debug: noop,
		info: noop,
		warn: noop,
		error: noop
	} as unknown as ILogger
}

/**
 * Recursively revive objects that were sanitized by the child's
 * sanitizeForPostMessage (e.g. Error sentinels) or decomposed by
 * structured clone (e.g. Long → { low, high, unsigned }).
 *
 * IMPORTANT: structured clone already produces a perfect copy of the data.
 * We only need to recursively look for __error__ sentinels and convert
 * them back to Error instances, and revive Long-like plain objects back
 * to real Long instances. Everything else passes through unchanged.
 */
function revivePostMessage(value: unknown): any {
	if (value !== null && typeof value === 'object') {
		if ((value as any).__error__) {
			const err = new Error((value as any).message)
			err.name = (value as any).name
			err.stack = (value as any).stack
			return err
		}

		// Revive Long-like objects that were decomposed by structured clone.
		// protobufjs uses Long for 64-bit integers; after postMessage they
		// become { low, high, unsigned } plain objects.
		if (isLongLike(value)) {
			return new Long((value as any).low, (value as any).high, (value as any).unsigned)
		}

		if (Array.isArray(value)) {
			// Arrays: recurse to revive any Error sentinels / Longs inside
			let changed = false
			const result = value.map(v => {
				const revived = revivePostMessage(v)
				if (revived !== v) changed = true
				return revived
			})
			return changed ? result : value
		}
		// Plain objects: only recurse if they might contain __error__ sentinels
		// or Long-like objects.
		if (value.constructor === Object || value.constructor === undefined) {
			let changed = false
			const result: any = {}
			for (const key of Object.keys(value as object)) {
				const orig = (value as any)[key]
				const revived = revivePostMessage(orig)
				if (revived !== orig) changed = true
				result[key] = revived
			}
			return changed ? result : value
		}
	}
	return value
}

/**
 * Check whether a value looks like a protobufjs Long that was decomposed
 * by structured clone. Long instances have exactly { low, high, unsigned }
 * after crossing postMessage.
 */
function isLongLike(value: unknown): boolean {
	if (value === null || typeof value !== 'object') return false
	const keys = Object.keys(value as object)
	return (
		keys.length === 3 &&
		keys.includes('low') &&
		keys.includes('high') &&
		keys.includes('unsigned') &&
		typeof (value as any).low === 'number' &&
		typeof (value as any).high === 'number' &&
		typeof (value as any).unsigned === 'boolean'
	)
}

// ---------------------------------------------------------------------------
// Pool management
// ---------------------------------------------------------------------------

function getStats(): PoolStats {
	const active = slots.filter(s => s.load > 0).length
	return {
		size: slots.length,
		active,
		idle: slots.length - active,
		totalSockets: slots.reduce((sum, s) => sum + s.load, 0)
	}
}

/**
 * Find the best worker to place a new socket on.
 * Strategy: pick the worker with the lowest load; if all are at capacity
 * and we haven't hit maxWorkers, spawn a new one.
 */
function acquireSlot(): WorkerSlot {
	// Find least-loaded existing worker
	let best: WorkerSlot | undefined = slots[0]
	for (let i = 1; i < slots.length; i++) {
		if (slots[i].load < best!.load) {
			best = slots[i]
		}
	}

	// If the best worker already has sockets and we can grow, spawn a new one
	if (best && best.load > 0 && slots.length < maxWorkers) {
		return spawnWorker()
	}

	if (!best) {
		return spawnWorker()
	}

	return best
}

function spawnWorker(): WorkerSlot {
	const worker = new Worker(workerScript, {
		execArgv: workerExecArgv ? [...workerExecArgv] : undefined
	})

	const slot: WorkerSlot = {
		worker,
		load: 0,
		emitters: new Map(),
		wsEmitters: new Map(),
		props: new Map(),
		callbacks: new Map(),
		keystores: new Map(),
		loggers: new Map(),
		configs: new Map(),
		pending: new Map()
	}

	// Global message handler — dispatches to the right socket's emitter
	worker.on('message', (msg: any) => {
		if (!msg || typeof msg !== 'object') return

		const socketId = msg.socketId as number | undefined
		const log = socketId !== undefined ? slot.loggers.get(socketId) : undefined

		switch (msg.type) {
			case 'event':
				if (socketId !== undefined) {
					const ev = slot.emitters.get(socketId)
					if (ev) {
						const map = revivePostMessage(msg.map as Record<string, unknown>)
						log?.trace({ eventKeys: Object.keys(map) }, 'worker event received')

						// Sync creds.update back to the parent's auth.creds object.
						// The worker mutates its own copy; without this the parent's
						// copy stays stale and saveCreds() writes old data.
						if (map['creds.update']) {
							const cfg = slot.configs.get(socketId)
							if (cfg?.auth?.creds) {
								Object.assign(cfg.auth.creds, map['creds.update'])
								log?.debug({ me: (cfg.auth.creds as any).me?.id }, 'synced creds.update to parent')
							}

							// Also update localProps.user so wsocket.user and its
							// nested properties (id, lid, name, verifiedName, etc.)
							// stay in sync with the worker's authState.creds.me.
							const credsUpdate = map['creds.update'] as Record<string, unknown> | undefined
							if (credsUpdate?.me) {
								const props = slot.props.get(socketId)
								if (props) {
									props.user = { ...(props.user as any), ...(credsUpdate.me as object) }
									log?.debug({ me: (credsUpdate.me as any)?.id }, 'synced user to localProps')
								}
							}
						}

						// Emit the aggregated 'event' for ev.process() compatibility
						ev.emit('event', map)
						// Also emit individual typed events for ev.on() listeners
						for (const [event, data] of Object.entries(map)) {
							ev.emit(event, data)
						}

						// When the socket closes, clean up proxy-side resources.
						// The worker already removed its entry; we remove the
						// emitter so a future startSock() can create a fresh one.
						if (map['connection.update']?.connection === 'close') {
							cleanupSocket(slot, socketId, log || createNoopLogger())
						}
					}
				}
				break

			case 'ws-event':
				if (socketId !== undefined) {
					const wsEv = slot.wsEmitters.get(socketId)
					if (wsEv) {
						const { event, args } = msg as { event: string; args: unknown[] }
						wsEv.emit(event, ...args)
					}
				}
				break

			case 'socket-info':
				if (socketId !== undefined) {
					const props = slot.props.get(socketId)
					if (props && msg.info) {
						Object.assign(props, msg.info)
					}
				}
				break

			case 'result': {
				const p = slot.pending.get(msg.id)
				if (p) {
					slot.pending.delete(msg.id)
					if (msg.error) {
						log?.debug({ rpcId: msg.id, error: msg.error }, 'worker RPC error')
						p.reject(new Error(msg.error))
					} else {
						log?.trace({ rpcId: msg.id }, 'worker RPC result')
						p.resolve(revivePostMessage(msg.result))
					}
				}
				break
			}

			case 'callback-call':
				if (socketId !== undefined) {
					log?.trace({ callbackKey: msg.key, rpcId: msg.id }, 'worker callback call received')
					handleCallbackCall(slot, socketId, msg)
				}
				break
		}
	})

	worker.on('error', err => {
		// Log to all socket loggers on this worker
		for (const log of slot.loggers.values()) {
			log.error({ err, workerPid: worker.threadId }, 'worker thread error')
		}
		const idx = slots.indexOf(slot)
		if (idx !== -1) slots.splice(idx, 1)
	})

	worker.on('exit', code => {
		for (const log of slot.loggers.values()) {
			log.info({ exitCode: code, workerPid: worker.threadId }, 'worker thread exited')
		}
		const idx = slots.indexOf(slot)
		if (idx !== -1) slots.splice(idx, 1)
	})

	slots.push(slot)
	return slot
}

async function handleCallbackCall(
	slot: WorkerSlot,
	socketId: number,
	msg: { id: number; key: string; args: unknown[] }
) {
	const { id, key, args } = msg
	const log = slot.loggers.get(socketId)

	// Handle keystore operations (auth.keys.get / auth.keys.set)
	if (key.startsWith('keystore.')) {
		const method = key.slice('keystore.'.length)
		const keystore = slot.keystores.get(socketId)
		if (!keystore || typeof keystore[method] !== 'function') {
			log?.error({ keystoreMethod: method }, 'no keystore method registered')
			return slot.worker.postMessage({
				type: 'callback-result',
				socketId,
				id,
				error: `No keystore method "${method}"`
			})
		}
		try {
			log?.debug({ keystoreMethod: method, argCount: args.length }, 'invoking keystore method')
			const result = await keystore[method](...args)
			slot.worker.postMessage({ type: 'callback-result', socketId, id, result })
		} catch (err: any) {
			log?.error({ err, keystoreMethod: method }, 'keystore method threw error')
			slot.worker.postMessage({
				type: 'callback-result',
				socketId,
				id,
				error: err?.message || String(err)
			})
		}
		return
	}

	// Handle regular config callbacks
	const store = slot.callbacks.get(socketId)
	const fn = store?.[key as CallbackKey]

	if (!fn) {
		log?.error({ callbackKey: key }, 'no callback registered for key')
		return slot.worker.postMessage({
			type: 'callback-result',
			socketId,
			id,
			error: `No callback registered for "${key}"`
		})
	}
	try {
		log?.debug({ callbackKey: key, argCount: args.length }, 'invoking proxied callback')
		const result = await fn(...args)
		slot.worker.postMessage({ type: 'callback-result', socketId, id, result })
	} catch (err: any) {
		log?.error({ err, callbackKey: key }, 'proxied callback threw error')
		slot.worker.postMessage({
			type: 'callback-result',
			socketId,
			id,
			error: err?.message || String(err)
		})
	}
}

/**
 * Resize the pool. If shrinking, idle workers are terminated first.
 * Active workers are never forcefully killed by resize.
 */
async function resizePool(newSize: number): Promise<void> {
	if (newSize < 1) newSize = 1
	const oldSize = maxWorkers
	maxWorkers = newSize

	// Log to all active loggers
	const allLoggers = slots.flatMap(s => [...s.loggers.values()])
	for (const log of allLoggers) {
		log.info({ oldSize, newSize, currentWorkers: slots.length }, 'pool resized')
	}

	// Terminate excess idle workers
	while (slots.length > maxWorkers) {
		const idleSlot = slots.find(s => s.load === 0)
		if (!idleSlot) break // all busy, can't shrink further
		const idx = slots.indexOf(idleSlot)
		slots.splice(idx, 1)
		for (const log of idleSlot.loggers.values()) {
			log.info({ workerPid: idleSlot.worker.threadId }, 'terminating idle worker (pool shrink)')
		}
		await idleSlot.worker.terminate()
	}
}

/**
 * Drain the pool: wait until all sockets are closed, then terminate all workers.
 */
async function drainPool(): Promise<void> {
	const allLoggers = slots.flatMap(s => [...s.loggers.values()])
	for (const log of allLoggers) {
		log.info({ workerCount: slots.length }, 'draining pool — waiting for all sockets to close')
	}

	// Wait for all sockets to close (load drops to 0)
	await new Promise<void>(resolve => {
		const check = () => {
			if (slots.every(s => s.load === 0)) {
				resolve()
			} else {
				setTimeout(check, 100)
			}
		}
		check()
	})

	// Terminate all workers
	for (const log of allLoggers) {
		log.info({ workerCount: slots.length }, 'all sockets closed — terminating workers')
	}
	await Promise.all(slots.map(s => s.worker.terminate()))
	slots.length = 0
}

// ---------------------------------------------------------------------------
// makeWASocket (pool-aware proxy)
// ---------------------------------------------------------------------------

/**
 * Recursively clone a value, replacing all functions with the sentinel
 * string '__proxy_fn__'. This ensures the config can be sent via
 * postMessage (structured clone).
 */
function deepStripFunctions(value: unknown, seen = new WeakSet()): any {
	if (typeof value === 'function') {
		return '__proxy_fn__'
	}
	if (value !== null && typeof value === 'object') {
		if (seen.has(value as object)) return '[Circular]'
		seen.add(value as object)

		// Preserve binary/typed-array types — structured clone handles them natively
		if (
			Buffer.isBuffer(value) ||
			value instanceof Uint8Array ||
			value instanceof ArrayBuffer ||
			ArrayBuffer.isView(value)
		) {
			return value
		}

		if (Array.isArray(value)) {
			return value.map(v => deepStripFunctions(v, seen))
		}
		const result: any = {}
		for (const key of Object.keys(value as object)) {
			result[key] = deepStripFunctions((value as any)[key], seen)
		}
		return result
	}
	return value
}

const makeWASocket = (config: UserFacingSocketConfig) => {
	const socketId = nextSocketId()

	// ---- Create a child logger for this socket ---------------------------
	const log: ILogger = config.logger ? config.logger.child({ worker: 'proxy', socketId }) : createNoopLogger()

	// ---- Extract non-serializable callbacks ------------------------------
	const callbackStore: Partial<Record<CallbackKey, Function>> = {}

	// Save the real keystore before stripping
	const realKeystore = (config as any).auth?.keys

	// Deep-strip all functions from the config so it can be sent via postMessage.
	// Functions are replaced with the sentinel string '__proxy_fn__'.
	// The child will create forwarding stubs for known callback keys and
	// the keystore; other stripped functions become no-ops.
	//
	// makeSignalRepository returns a complex object (closures + methods) that
	// cannot be cloned back to the worker. We replace it with a sentinel and
	// recreate the default libsignal repository inside the worker.
	const configWithSignalRepo =
		typeof (config as any).makeSignalRepository === 'function'
			? { ...config, makeSignalRepository: '__worker_signal_repository__' as const }
			: config
	const serializableConfig: any = deepStripFunctions(configWithSignalRepo)

	// Mark the logger so the child knows to create a real one
	if (config.logger) {
		;(serializableConfig as any).logger = '__proxy_logger__'
	}

	// Mark the keystore so the child creates forwarding stubs
	if (realKeystore) {
		if (!serializableConfig.auth) serializableConfig.auth = {}
		serializableConfig.auth.keys = '__proxy_keystore__'
	}

	// Worker-local cache stores cannot be cloned (their methods live on the
	// prototype and would become broken plain objects). Replace them with a
	// sentinel so the child creates a fresh cache inside the worker.
	for (const k of CACHE_CONFIG_KEYS) {
		if (typeof (config as any)[k]?.get === 'function') {
			;(serializableConfig as any)[k] = CACHE_SENTINEL
		}
	}

	// HTTP agents contain sockets and native state; they cannot cross
	// postMessage. The worker will create its own agents.
	if (serializableConfig.agent) {
		serializableConfig.agent = undefined
	}
	if (serializableConfig.fetchAgent) {
		serializableConfig.fetchAgent = undefined
	}

	// Restore known callback sentinels with the proper key names
	for (const k of CALLBACK_CONFIG_KEYS) {
		if (typeof (config as any)[k] === 'function') {
			callbackStore[k as CallbackKey] = (config as any)[k]
			;(serializableConfig as any)[k] = '__proxy_callback__'
		}
	}

	// ---- Acquire a worker slot -------------------------------------------
	const slot = acquireSlot()
	slot.load++
	slot.emitters.set(socketId, new EventEmitter())
	slot.wsEmitters.set(socketId, new EventEmitter())
	slot.props.set(socketId, {})
	slot.callbacks.set(socketId, callbackStore)
	slot.keystores.set(socketId, realKeystore)
	slot.loggers.set(socketId, log)
	slot.configs.set(socketId, config)

	log.info(
		{
			workerPid: slot.worker.threadId,
			workerLoad: slot.load,
			poolSize: slots.length,
			maxWorkers
		},
		'socket assigned to worker'
	)

	// ---- Send init message to the worker for this socket -----------------
	slot.worker.postMessage({
		type: 'init',
		socketId,
		config: serializableConfig
	})

	log.debug('init message sent to worker')

	// ---- Public event emitter (local to this socket) ---------------------
	const rawEv = slot.emitters.get(socketId)!

	// Wrap ev: .on/.off/.emit etc. are handled locally.
	// .process() is implemented locally (it just listens to the aggregated
	// 'event' that the worker forwards).
	// .buffer/.flush/.createBufferedFunction/.isBuffering are forwarded to
	// the worker via RPC.
	const ev = new Proxy(rawEv, {
		get(target, prop: string) {
			// Local EventEmitter methods
			if (
				prop === 'on' ||
				prop === 'off' ||
				prop === 'emit' ||
				prop === 'removeListener' ||
				prop === 'addListener' ||
				prop === 'removeAllListeners' ||
				prop === 'listeners' ||
				prop === 'listenerCount' ||
				prop === 'eventNames' ||
				prop === 'getMaxListeners' ||
				prop === 'setMaxListeners' ||
				prop === 'rawListeners' ||
				prop === 'prependListener' ||
				prop === 'prependOnceListener' ||
				prop === 'once'
			) {
				return (target as any)[prop].bind(target)
			}

			// process() is implemented locally: it listens to the aggregated
			// 'event' that the worker forwards from the real socket.
			if (prop === 'process') {
				return (handler: (events: Record<string, unknown>) => void | Promise<void>) => {
					const listener = (map: Record<string, unknown>) => {
						handler(map)
					}
					target.on('event', listener)
					return () => target.off('event', listener)
				}
			}

			// Forward other methods (buffer, flush, createBufferedFunction,
			// isBuffering) to the worker via RPC
			return (...args: unknown[]) => rpcCall(`ev.${prop}`, args)
		}
	})

	// ---- RPC helper ------------------------------------------------------
	const rpcCall = (method: string, args: unknown[]): Promise<unknown> => {
		const id = nextReqId()
		log.trace({ rpcId: id, method, argCount: args.length }, 'RPC call → worker')
		return new Promise((resolve, reject) => {
			slot.pending.set(id, { socketId, resolve, reject })
			slot.worker.postMessage({ type: 'call', socketId, id, method, args })
		})
	}

	// ---- Build the proxy object ------------------------------------------
	const rawWs = slot.wsEmitters.get(socketId)!
	const localProps = slot.props.get(socketId)!

	// Wrap ws in a proxy: EventEmitter methods work locally,
	// close() is forwarded to the worker via RPC.
	const wsProxy = new Proxy(rawWs, {
		get(target, prop: string) {
			if (prop === 'close') {
				return (...args: unknown[]) => {
					if (!isSocketActive(slot, socketId)) {
						log.debug('ws.close() ignored — socket already cleaned up')
						return Promise.resolve()
					}
					return rpcCall('ws.close', args)
				}
			}
			// Delegate everything else (on, off, removeAllListeners, etc.) to the EventEmitter
			const value = (target as any)[prop]
			if (typeof value === 'function') {
				return value.bind(target)
			}
			return value
		}
	})

	const socketProxy = new Proxy(
		{ ev, ws: wsProxy },
		{
			get(_target, prop: string) {
				if (prop === 'ev') return ev
				if (prop === 'ws') return wsProxy
				if (prop === 'then') return undefined
				if (prop === 'pool') return getStats()

				// Check local property store first (wsocket.id, wsocket.user, etc.)
				if (prop in localProps) return localProps[prop]

				if (prop === 'end') {
					return async (...args: unknown[]) => {
						if (!isSocketActive(slot, socketId)) {
							log.debug('socket.end() ignored — already cleaned up')
							return
						}
						log.info('socket.end() called — cleaning up')
						try {
							await rpcCall('end', args)
						} catch (_) {
							/* ok */
						}
						cleanupSocket(slot, socketId, log)
					}
				}

				if (prop === 'logout') {
					return async (...args: unknown[]) => {
						if (!isSocketActive(slot, socketId)) {
							log.debug('socket.logout() ignored — already cleaned up')
							return
						}
						log.info('socket.logout() called — cleaning up')
						try {
							await rpcCall('logout', args)
						} catch (_) {
							/* ok */
						}
						cleanupSocket(slot, socketId, log)
					}
				}

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

				return (...args: unknown[]) => rpcCall(prop, args)
			},

			set(_target, prop: string, value: unknown) {
				localProps[prop] = value
				return true
			}
		}
	) as any

	return socketProxy
}

function isSocketActive(slot: WorkerSlot, socketId: number): boolean {
	return slot.emitters.has(socketId)
}

function cleanupSocket(slot: WorkerSlot, socketId: number, log: ILogger): boolean {
	// Idempotent cleanup: only run once per socketId.
	if (!isSocketActive(slot, socketId)) {
		return false
	}

	// Reject any pending RPC calls for this socket so they don't hang or
	// receive the worker's "Socket X not found" error after reconnection.
	for (const [id, p] of slot.pending) {
		if (p.socketId === socketId) {
			slot.pending.delete(id)
			p.reject(new Error(`Socket ${socketId} closed`))
		}
	}

	slot.emitters.delete(socketId)
	slot.wsEmitters.delete(socketId)
	slot.props.delete(socketId)
	slot.callbacks.delete(socketId)
	slot.keystores.delete(socketId)
	slot.loggers.delete(socketId)
	slot.configs.delete(socketId)
	slot.load = Math.max(0, slot.load - 1)

	log.info({ workerLoad: slot.load, poolSize: slots.length }, 'socket cleaned up')
	return true

	// If the worker is now idle and we're over capacity, terminate it
	if (slot.load === 0 && slots.length > maxWorkers) {
		const idx = slots.indexOf(slot)
		if (idx !== -1) {
			slots.splice(idx, 1)
			log.info({ workerPid: slot.worker.threadId }, 'terminating idle worker (over capacity)')
			slot.worker.terminate()
		}
	}
}

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------
export default makeWASocket
export { makeWASocket, getStats, resizePool, drainPool }
