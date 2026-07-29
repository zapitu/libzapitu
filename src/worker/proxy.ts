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

const workerScript = join(__dirname, 'child.js')

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
	const worker = new Worker(workerScript)

	const slot: WorkerSlot = {
		worker,
		load: 0,
		emitters: new Map(),
		callbacks: new Map(),
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
						log?.trace({ event: msg.event, dataKeys: msg.data ? Object.keys(msg.data) : [] }, 'worker event received')
						ev.emit(msg.event, msg.data)
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
	msg: { id: number; key: CallbackKey; args: unknown[] }
) {
	const { id, key, args } = msg
	const store = slot.callbacks.get(socketId)
	const fn = store?.[key]
	const log = slot.loggers.get(socketId)

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
const makeWASocket = (config: UserFacingSocketConfig) => {
	const socketId = nextSocketId()

	// ---- Create a child logger for this socket ---------------------------
	const log: ILogger = config.logger
		? config.logger.child({ worker: 'proxy', socketId })
		: createNoopLogger()

	// ---- Extract non-serializable callbacks ------------------------------
	const callbackStore: Partial<Record<CallbackKey, Function>> = {}
	const serializableConfig: any = { ...config }

	for (const k of CALLBACK_CONFIG_KEYS) {
		if (typeof (serializableConfig as any)[k] === 'function') {
			callbackStore[k as CallbackKey] = (serializableConfig as any)[k]
			;(serializableConfig as any)[k] = '__proxy_callback__'
		}
	}

	// ---- Acquire a worker slot -------------------------------------------
	const slot = acquireSlot()
	slot.load++
	slot.emitters.set(socketId, new EventEmitter())
	slot.callbacks.set(socketId, callbackStore)
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
	const ev = slot.emitters.get(socketId)!

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
