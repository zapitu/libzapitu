/**
 * Worker child entry point.
 * Runs makeWASocket inside a worker_threads context and bridges all
 * method calls and events between the parent and the WA socket.
 *
 * Supports multiple concurrent sockets per worker — each identified
 * by a unique `socketId` sent in the `init` message from the parent.
 */
import { isMainThread, parentPort } from 'worker_threads'
import makeWASocket from '../Socket'
import type { UserFacingSocketConfig } from '../Types'
import type { BaileysEventMap } from '../Types/Events'
import type { ILogger } from '../Utils/logger'

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
	'shouldSyncHistoryMessage',
	'makeSignalRepository',
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
		error: noop,
	} as unknown as ILogger
}

// ---------------------------------------------------------------------------
// Callback proxy helper
// ---------------------------------------------------------------------------
function proxyCallback(
	socketId: number,
	key: ProxiedCallbackKey,
	log: ILogger,
	...args: unknown[]
): Promise<unknown> {
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
function createSocket(socketId: number, rawConfig: any): void {
	const config: UserFacingSocketConfig = rawConfig

	// Create a child logger for this socket on the worker side
	const log: ILogger = config.logger
		? config.logger.child({ worker: 'child', socketId })
		: createNoopLogger()

	log.info('creating socket in worker')

	// Replace sentinel callbacks with forwarding stubs
	for (const k of CALLBACK_PROXIED_KEYS) {
		if ((config as any)[k] === '__proxy_callback__') {
			;(config as any)[k] = (...args: unknown[]) => proxyCallback(socketId, k, log, ...args)
		}
	}

	const sock = makeWASocket(config)

	log.info('socket created successfully')

	// Forward ALL events to the parent, tagged with socketId
	;(sock.ev as any).on('event', (map: Partial<BaileysEventMap>) => {
		for (const [event, data] of Object.entries(map)) {
			log.trace({ event, dataKeys: data ? Object.keys(data) : [] }, 'event → parent')
			parentPort!.postMessage({ type: 'event', socketId, event, data })
		}
	})

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
					error: `Socket ${socketId} not found`,
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
					parentPort!.postMessage({ type: 'result', socketId, id, result })
				} else {
					log.trace({ rpcId: id, method }, 'RPC property read')
					parentPort!.postMessage({ type: 'result', socketId, id, result: target })
				}
			} catch (err: any) {
				log.error({ err, rpcId: id, method }, 'RPC call failed')
				parentPort!.postMessage({
					type: 'result',
					socketId,
					id,
					error: err?.message || String(err),
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
