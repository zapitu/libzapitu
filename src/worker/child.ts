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
}

const sockets = new Map<number, SocketEntry>()

let _reqId = 0
const nextReqId = (): number => ++_reqId

// ---------------------------------------------------------------------------
// Callback proxy helper
// ---------------------------------------------------------------------------
function proxyCallback(
	socketId: number,
	key: ProxiedCallbackKey,
	...args: unknown[]
): Promise<unknown> {
	const id = nextReqId()
	return new Promise((resolve, reject) => {
		const onMsg = (msg: any) => {
			if (msg?.type === 'callback-result' && msg.id === id && msg.socketId === socketId) {
				parentPort!.off('message', onMsg)
				if (msg.error) {
					reject(new Error(msg.error))
				} else {
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

	// Replace sentinel callbacks with forwarding stubs
	for (const k of CALLBACK_PROXIED_KEYS) {
		if ((config as any)[k] === '__proxy_callback__') {
			;(config as any)[k] = (...args: unknown[]) => proxyCallback(socketId, k, ...args)
		}
	}

	const sock = makeWASocket(config)

	// Forward ALL events to the parent, tagged with socketId
	;(sock.ev as any).on('event', (map: Partial<BaileysEventMap>) => {
		for (const [event, data] of Object.entries(map)) {
			parentPort!.postMessage({ type: 'event', socketId, event, data })
		}
	})

	sockets.set(socketId, { sock, config })
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

			try {
				let target: any = entry.sock
				const path = method.split('.')
				for (const segment of path) {
					if (target === null || target === undefined) {
						throw new Error(`Cannot read property '${segment}' of ${target}`)
					}
					target = target[segment]
				}

				if (typeof target === 'function') {
					const thisCtx = path.length > 1 ? resolveTarget(entry.sock, path.slice(0, -1)) : entry.sock
					const result = await target.apply(thisCtx, args || [])
					parentPort!.postMessage({ type: 'result', socketId, id, result })
				} else {
					parentPort!.postMessage({ type: 'result', socketId, id, result: target })
				}
			} catch (err: any) {
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
