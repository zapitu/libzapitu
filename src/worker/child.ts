/**
 * Worker child entry point.
 * Runs makeWASocket inside a worker_threads context and bridges all
 * method calls and events between the parent and the WA socket.
 *
 * Supports multiple concurrent sockets per worker — each identified
 * by a unique `socketId` sent in the `init` message from the parent.
 */
import { isMainThread, parentPort } from 'worker_threads'
import NodeCache from '@cacheable/node-cache'
import makeWASocket from '../Socket'
import { makeLibSignalRepository } from '../Signal/libsignal'
import type { UserFacingSocketConfig } from '../Types'
import type { BaileysEventMap } from '../Types/Events'
import defaultLogger, { type ILogger } from '../Utils/logger'

// ---------------------------------------------------------------------------
// Ensure we never run this in the main thread
// ---------------------------------------------------------------------------
if (isMainThread || !parentPort) {
	throw new Error('This file must be run as a Worker (worker_threads).')
}

// ---------------------------------------------------------------------------
// Per-socket state
// ---------------------------------------------------------------------------
const CALLBACK_PROXIED_KEYS = [
	'getMessage',
	'shouldIgnoreJid',
	'patchMessageBeforeSending',
	'cachedGroupMetadata',
	'shouldSyncHistoryMessage'
] as const

type ProxiedCallbackKey = (typeof CALLBACK_PROXIED_KEYS)[number]

interface SocketEntry {
	sock: ReturnType<typeof makeWASocket>
	config: UserFacingSocketConfig
	log: ILogger
}

const sockets = new Map<number, SocketEntry>()

let _reqId = 0
const nextReqId = (): number => ++_reqId

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

// ---------------------------------------------------------------------------
// Callback proxy helper
// ---------------------------------------------------------------------------
function proxyCallback(socketId: number, key: ProxiedCallbackKey, log: ILogger, ...args: unknown[]): Promise<unknown> {
	const id = nextReqId()
	log.trace({ callbackKey: key, rpcId: id }, 'callback → parent')
	return new Promise((resolve, reject) => {
		const onMsg = (msg: any) => {
			if (msg?.type === 'callback-result' && msg.id === id && msg.socketId === socketId) {
				parentPort!.off('message', onMsg)
				if (msg.error) {
					log.debug({ callbackKey: key, rpcId: id, error: msg.error }, 'callback error from parent')
					reject(new Error(msg.error))
				} else {
					log.trace({ callbackKey: key, rpcId: id }, 'callback result from parent')
					resolve(msg.result)
				}
			}
		}
		parentPort!.on('message', onMsg)
		parentPort!.postMessage({ type: 'callback-call', socketId, id, key, args })
	})
}

// ---------------------------------------------------------------------------
// Create a socket for a given socketId
// ---------------------------------------------------------------------------

/**
 * Recursively clone a value for postMessage, replacing non-cloneable types
 * (Error, Function) with plain serializable representations and converting
 * protobufjs message instances to JSON.
 *
 * Protobuf messages (e.g. proto.Message, proto.WebMessageInfo) carry a
 * toJSON() method that serializes byte fields as base64 strings and 64-bit
 * integer fields as strings. Structured clone strips the prototype chain, so
 * without this step Buffers become Uint8Array plain objects and Longs become
 * { low, high, unsigned } objects on the parent side. Converting them before
 * crossing the boundary keeps the worker event shape identical to direct mode
 * when the data is logged or JSON.stringify'd.
 */
function serializeForPostMessage(value: unknown, seen = new WeakSet()): any {
	if (value instanceof Error) {
		return { __error__: true, message: value.message, name: value.name, stack: value.stack }
	}
	if (typeof value === 'function') {
		return '__fn__'
	}
	if (value !== null && typeof value === 'object') {
		if (seen.has(value as object)) return '[Circular]'
		seen.add(value as object)

		// Detect a protobufjs message instance by the static methods its
		// constructor exposes, then use the instance's own toJSON() so nested
		// bytes/long fields are serialized consistently. Tag the resulting
		// plain object with the constructor name so the parent can revive it
		// back into a real protobuf instance (and therefore recover the
		// runtime Buffer types downstream code expects).
		const ctor = (value as any).constructor
		if (
			ctor &&
			typeof ctor === 'function' &&
			typeof ctor.encode === 'function' &&
			typeof ctor.decode === 'function' &&
			typeof ctor.toObject === 'function' &&
			typeof (value as any).toJSON === 'function'
		) {
			const json = (value as any).toJSON()
			json.__protobufType__ = ctor.name
			return serializeForPostMessage(json, seen)
		}

		if (Array.isArray(value)) {
			return value.map(v => serializeForPostMessage(v, seen))
		}
		// Preserve binary/typed-array types — structured clone handles them natively
		if (
			Buffer.isBuffer(value) ||
			value instanceof Uint8Array ||
			value instanceof ArrayBuffer ||
			ArrayBuffer.isView(value)
		) {
			return value
		}
		const result: any = {}
		for (const key of Object.keys(value as object)) {
			result[key] = serializeForPostMessage((value as any)[key], seen)
		}
		return result
	}
	return value
}

/**
 * Recursively convert Uint8Array values to Buffer.
 * After structured clone via postMessage, Uint8Arrays survive as Uint8Array,
 * but libsignal and other libraries expect Buffer instances.
 */
function reviveBuffers(value: unknown, seen = new WeakSet()): any {
	if (value instanceof Uint8Array && !Buffer.isBuffer(value)) {
		return Buffer.from(value)
	}
	if (value !== null && typeof value === 'object') {
		if (seen.has(value as object)) return value
		seen.add(value as object)
		if (Array.isArray(value)) {
			return value.map(v => reviveBuffers(v, seen))
		}
		const result: any = {}
		for (const key of Object.keys(value as object)) {
			result[key] = reviveBuffers((value as any)[key], seen)
		}
		return result
	}
	return value
}

function createSocket(socketId: number, rawConfig: any): void {
	const config: UserFacingSocketConfig = reviveBuffers(rawConfig)

	// The proxy replaces the logger with a sentinel string because functions
	// can't be serialized across postMessage. Create a real logger here.
	// Use the default logger (level controlled by BAILEYS_LOG_LEVEL) so the
	// real makeWASocket has a working logger.
	const hasLogger = (config as any).logger === '__proxy_logger__'
	const realLogger: ILogger = hasLogger ? defaultLogger.child({ worker: 'child', socketId }) : createNoopLogger()
	;(config as any).logger = realLogger

	// Create a child logger for our own worker-level logging
	const log: ILogger = realLogger.child({ component: 'worker-bridge' })

	log.info('creating socket in worker')

	// Replace sentinel callbacks with forwarding stubs
	for (const k of CALLBACK_PROXIED_KEYS) {
		if ((config as any)[k] === '__proxy_callback__') {
			;(config as any)[k] = (...args: unknown[]) => proxyCallback(socketId, k, log, ...args)
		}
	}

	// Recreate the signal repository inside the worker. The repository object
	// contains closures and methods that cannot cross postMessage, so the parent
	// sends a sentinel instead of proxying the factory.
	if ((config as any).makeSignalRepository === '__worker_signal_repository__') {
		;(config as any).makeSignalRepository = makeLibSignalRepository
	}

	// Replace sentinel keystore with forwarding stubs
	if ((config as any).auth?.keys === '__proxy_keystore__') {
		const keystoreMethods = ['get', 'set', 'clear', 'isInTransaction', 'transaction']
		const stub: any = {}
		for (const method of keystoreMethods) {
			stub[method] = (...args: unknown[]) => proxyCallback(socketId, `keystore.${method}` as any, log, ...args)
		}
		;(config as any).auth.keys = stub
	}

	// Replace worker-local cache sentinels with real cache instances.
	// The parent's cache cannot be cloned (methods live on prototype).
	const cacheKeys = [
		'msgRetryCounterCache',
		'mediaCache',
		'userDevicesCache',
		'callOfferCache',
		'placeholderResendCache'
	]
	for (const k of cacheKeys) {
		if ((config as any)[k] === '__worker_cache__') {
			;(config as any)[k] = new NodeCache({ useClones: false })
		}
	}

	const sock = makeWASocket(config)

	log.info('socket created successfully')

	// Send user and authState to the parent when they become available.
	// These are needed for wsocket.user and wsocket.authState on the proxy side.
	const sendSocketInfo = () => {
		try {
			const info: any = {}
			if ((sock as any).user) info.user = (sock as any).user
			if ((sock as any).authState) info.authState = (sock as any).authState
			if (Object.keys(info).length > 0) {
				parentPort!.postMessage({ type: 'socket-info', socketId, info })
			}
		} catch (_) {
			/* best-effort */
		}
	}

	// Send immediately (may be empty) and also on connection.open.
	// IMPORTANT: we send socket-info synchronously in the connection.update
	// listener so it arrives at the parent BEFORE the forwarded event.
	// Otherwise wsocket.user would be undefined when the parent's
	// connection.update handler runs.
	sendSocketInfo()
	;(sock.ev as any).on('connection.update', (update: any) => {
		if (update?.connection === 'open') {
			sendSocketInfo()
		}

		// When the socket closes, clean up this socket entry so a new
		// init with a different socketId can reuse this worker.
		if (update?.connection === 'close') {
			const entry = sockets.get(socketId)
			if (entry) {
				log.info('socket closed — removing from worker')
				sockets.delete(socketId)
			}
		}
	})

	// Forward ALL events to the parent, tagged with socketId.
	// Send the aggregated map so the parent can emit both the 'event'
	// aggregate (for ev.process()) and individual typed events.
	// Protobuf message instances are converted to JSON first so the worker
	// boundary does not turn their byte/long fields into Uint8Arrays / Long
	// plain objects.
	;(sock.ev as any).on('event', (map: Partial<BaileysEventMap>) => {
		log.trace({ eventKeys: Object.keys(map) }, 'event → parent')
		try {
			parentPort!.postMessage({ type: 'event', socketId, map: serializeForPostMessage(map) })
		} catch (err: any) {
			log.error({ err, eventKeys: Object.keys(map) }, 'failed to send event to parent (non-cloneable data)')
		}
	})

	// Forward raw WS events to the parent so ws.on() works on the proxy side.
	// We forward all events by monkey-patching sock.ws.emit.
	const ws = (sock as any).ws
	if (ws) {
		const origEmit = ws.emit.bind(ws)
		ws.emit = (event: string, ...args: unknown[]) => {
			// Forward to parent first (best-effort, non-cloneable data is dropped)
			try {
				parentPort!.postMessage({ type: 'ws-event', socketId, event, args })
			} catch (_) {
				// silently drop — ws events with non-cloneable data are rare
			}
			// Then call the original emit so local listeners still work
			return origEmit(event, ...args)
		}
	}

	sockets.set(socketId, { sock, config, log })
}

// ---------------------------------------------------------------------------
// Handle incoming messages from the parent
// ---------------------------------------------------------------------------
parentPort.on('message', async (msg: any) => {
	if (!msg || typeof msg !== 'object') return

	switch (msg.type) {
		// --- Initialize a new socket on this worker ---
		case 'init': {
			const { socketId, config } = msg
			if (sockets.has(socketId)) {
				// Already initialized — ignore duplicate
				return
			}
			createSocket(socketId, config)
			break
		}

		// --- RPC method call ---
		case 'call': {
			const { socketId, id, method, args } = msg
			const entry = sockets.get(socketId)
			if (!entry) {
				parentPort!.postMessage({
					type: 'result',
					socketId,
					id,
					error: `Socket ${socketId} not found`
				})
				return
			}

			const { sock, log } = entry
			log.trace({ rpcId: id, method, argCount: args?.length ?? 0 }, 'RPC call received')

			try {
				let target: any = sock
				const path = method.split('.')
				for (const segment of path) {
					if (target === null || target === undefined) {
						throw new Error(`Cannot read property '${segment}' of ${target}`)
					}
					target = target[segment]
				}

				if (typeof target === 'function') {
					const thisCtx = path.length > 1 ? resolveTarget(sock, path.slice(0, -1)) : sock
					const result = await target.apply(thisCtx, args || [])
					log.trace({ rpcId: id, method }, 'RPC call succeeded')
					parentPort!.postMessage({ type: 'result', socketId, id, result: serializeForPostMessage(result) })
				} else {
					log.trace({ rpcId: id, method }, 'RPC property read')
					parentPort!.postMessage({ type: 'result', socketId, id, result: serializeForPostMessage(target) })
				}
			} catch (err: any) {
				log.error({ err, rpcId: id, method }, 'RPC call failed')
				parentPort!.postMessage({
					type: 'result',
					socketId,
					id,
					error: err?.message || String(err)
				})
			}
			break
		}
	}
})

/**
 * Walk a dot-separated path on the socket to resolve a nested target
 * (used as `this` context for method calls).
 */
function resolveTarget(obj: any, path: string[]): any {
	let t = obj
	for (const seg of path) {
		t = t[seg]
	}
	return t
}
