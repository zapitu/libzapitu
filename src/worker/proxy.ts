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
import { existsSync } from 'fs'
import { join } from 'path'
import type { UserFacingSocketConfig } from '../Types'
import type { ILogger } from '../Utils/logger'

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------
const CALLBACK_CONFIG_KEYS = [
	'getMessage',
	'shouldIgnoreJid',
	'patchMessageBeforeSending',
	'cachedGroupMetadata',
	'shouldSyncHistoryMessage',
	'makeSignalRepository',
] as const

type CallbackKey = (typeof CALLBACK_CONFIG_KEYS)[number]

interface WorkerSlot {
	worker: Worker
	/** Number of active sockets on this worker */
	load: number
	/** Per-socket event emitters keyed by a unique socketId */
	emitters: Map<number, EventEmitter>
	/** Per-socket callback stores */
	callbacks: Map<number, Partial<Record<CallbackKey, Function>>>
	/** Per-socket real auth.keys (SignalKeyStore) — proxied separately */
	keystores: Map<number, any>
	/** Per-socket loggers (child loggers from the user's config) */
	loggers: Map<number, ILogger>
	/** Pending RPC calls keyed by request id */
	pending: Map<number, { resolve: (v: any) => void; reject: (e: Error) => void }>
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
		error: noop,
	} as unknown as ILogger
}

/**
 * Recursively revive objects that were sanitized by the child's
 * sanitizeForPostMessage (e.g. Error sentinels).
 */
function revivePostMessage(value: unknown): any {
	if (value !== null && typeof value === 'object') {
		if ((value as any).__error__) {
			const err = new Error((value as any).message)
			err.name = (value as any).name
			err.stack = (value as any).stack
			return err
		}
		if (Array.isArray(value)) {
			return value.map(revivePostMessage)
		}
		const result: any = {}
		for (const key of Object.keys(value as object)) {
			result[key] = revivePostMessage((value as any)[key])
		}
		return result
	}
	return value
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
		totalSockets: slots.reduce((sum, s) => sum + s.load, 0),
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
		execArgv: workerExecArgv ? [...workerExecArgv] : undefined,
	})

	const slot: WorkerSlot = {
		worker,
		load: 0,
		emitters: new Map(),
		callbacks: new Map(),
		keystores: new Map(),
		loggers: new Map(),
		pending: new Map(),
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
						// Emit the aggregated 'event' for ev.process() compatibility
						ev.emit('event', map)
						// Also emit individual typed events for ev.on() listeners
						for (const [event, data] of Object.entries(map)) {
							ev.emit(event, data)
						}
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
						p.resolve(msg.result)
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

	worker.on('error', (err) => {
		// Log to all socket loggers on this worker
		for (const log of slot.loggers.values()) {
			log.error({ err, workerPid: worker.threadId }, 'worker thread error')
		}
		const idx = slots.indexOf(slot)
		if (idx !== -1) slots.splice(idx, 1)
	})

	worker.on('exit', (code) => {
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
				error: `No keystore method "${method}"`,
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
				error: err?.message || String(err),
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
			error: `No callback registered for "${key}"`,
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
			error: err?.message || String(err),
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
	const log: ILogger = config.logger
		? config.logger.child({ worker: 'proxy', socketId })
		: createNoopLogger()

	// ---- Extract non-serializable callbacks ------------------------------
	const callbackStore: Partial<Record<CallbackKey, Function>> = {}

	// Save the real keystore before stripping
	const realKeystore = (config as any).auth?.keys

	// Deep-strip all functions from the config so it can be sent via postMessage.
	// Functions are replaced with the sentinel string '__proxy_fn__'.
	// The child will create forwarding stubs for known callback keys and
	// the keystore; other stripped functions become no-ops.
	const serializableConfig: any = deepStripFunctions(config)

	// Mark the logger so the child knows to create a real one
	if (config.logger) {
		;(serializableConfig as any).logger = '__proxy_logger__'
	}

	// Mark the keystore so the child creates forwarding stubs
	if (realKeystore) {
		if (!serializableConfig.auth) serializableConfig.auth = {}
		serializableConfig.auth.keys = '__proxy_keystore__'
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
	slot.callbacks.set(socketId, callbackStore)
	slot.keystores.set(socketId, realKeystore)
	slot.loggers.set(socketId, log)

	log.info(
		{
			workerPid: slot.worker.threadId,
			workerLoad: slot.load,
			poolSize: slots.length,
			maxWorkers,
		},
		'socket assigned to worker'
	)

	// ---- Send init message to the worker for this socket -----------------
	slot.worker.postMessage({
		type: 'init',
		socketId,
		config: serializableConfig,
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
			if (prop === 'on' || prop === 'off' || prop === 'emit' ||
				prop === 'removeListener' || prop === 'addListener' ||
				prop === 'removeAllListeners' || prop === 'listeners' ||
				prop === 'listenerCount' || prop === 'eventNames' ||
				prop === 'getMaxListeners' || prop === 'setMaxListeners' ||
				prop === 'rawListeners' || prop === 'prependListener' ||
				prop === 'prependOnceListener' || prop === 'once') {
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
			slot.pending.set(id, { resolve, reject })
			slot.worker.postMessage({ type: 'call', socketId, id, method, args })
		})
	}

	// ---- Build the proxy object ------------------------------------------
	const socketProxy = new Proxy(
		{ ev },
		{
			get(_target, prop: string) {
				if (prop === 'ev') return ev
				if (prop === 'then') return undefined
				if (prop === 'pool') return getStats()

				if (prop === 'end') {
					return async (...args: unknown[]) => {
						log.info('socket.end() called — cleaning up')
						try { await rpcCall('end', args) } catch (_) { /* ok */ }
						cleanupSocket(slot, socketId, log)
					}
				}

				if (prop === 'logout') {
					return async (...args: unknown[]) => {
						log.info('socket.logout() called — cleaning up')
						try { await rpcCall('logout', args) } catch (_) { /* ok */ }
						cleanupSocket(slot, socketId, log)
					}
				}

				return (...args: unknown[]) => rpcCall(prop, args)
			},
		}
	) as any

	return socketProxy
}

function cleanupSocket(slot: WorkerSlot, socketId: number, log: ILogger): void {
	slot.emitters.delete(socketId)
	slot.callbacks.delete(socketId)
	slot.keystores.delete(socketId)
	slot.loggers.delete(socketId)
	slot.load = Math.max(0, slot.load - 1)

	log.info({ workerLoad: slot.load, poolSize: slots.length }, 'socket cleaned up')

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
