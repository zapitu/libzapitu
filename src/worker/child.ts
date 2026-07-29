/**
 * Worker child entry point.
 * Runs makeWASocket inside a worker_threads context and bridges all
 * method calls and events between the parent and the WA socket.
 */
import { isMainThread, parentPort, workerData } from 'worker_threads'
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
// Config with callback stubs replaced
// ---------------------------------------------------------------------------
const config: UserFacingSocketConfig = workerData.config

// Wrap any config callbacks that can't be serialized.
// The proxy side must inject stubs that send/receive messages to/from
// the parent so that non-serializable callbacks still work.
const CALLBACK_PROXIED_KEYS = [
	'getMessage',
	'shouldIgnoreJid',
	'patchMessageBeforeSending',
	'cachedGroupMetadata',
	'shouldSyncHistoryMessage',
	'makeSignalRepository',
] as const

type ProxiedCallbackKey = (typeof CALLBACK_PROXIED_KEYS)[number]

/**
 * Generate unique monotonic request IDs for RPC communication.
 */
let _reqId = 0
const nextReqId = (): number => ++_reqId

/**
 * Send a callback invocation to the parent and wait for the result.
 */
function proxyCallback<K extends ProxiedCallbackKey>(
	key: K,
	...args: unknown[]
): Promise<unknown> {
	const id = nextReqId()
	return new Promise((resolve, reject) => {
		const onMsg = (msg: any) => {
			if (msg?.type === 'callback-result' && msg.id === id) {
				parentPort!.off('message', onMsg)
				if (msg.error) {
					reject(new Error(msg.error))
				} else {
					resolve(msg.result)
				}
			}
		}
		parentPort!.on('message', onMsg)
		parentPort!.postMessage({ type: 'callback-call', id, key, args })
	})
}

// Replace proxied callbacks with forwarding stubs.
// The proxy replaces non-serializable callbacks with the sentinel string
// '__proxy_callback__' — if we see that value, install a forwarding stub.
for (const k of CALLBACK_PROXIED_KEYS) {
	if ((config as any)[k] === '__proxy_callback__') {
		;(config as any)[k] = (...args: unknown[]) => proxyCallback(k, ...args)
	}
}

// ---------------------------------------------------------------------------
// Create the socket
// ---------------------------------------------------------------------------
const sock = makeWASocket(config)

// ---------------------------------------------------------------------------
// Forward ALL events to the parent
// ---------------------------------------------------------------------------
// The event emitter is on sock.ev. We listen to all possible events by
// listening on the raw 'event' aggregated event, which fires before
// individual typed events.
;(sock.ev as any).on('event', (map: Partial<BaileysEventMap>) => {
	for (const [event, data] of Object.entries(map)) {
		parentPort!.postMessage({ type: 'event', event, data })
	}
})

// ---------------------------------------------------------------------------
// Handle incoming RPC method calls from the parent
// ---------------------------------------------------------------------------
parentPort.on('message', async (msg: any) => {
	if (!msg || msg.type !== 'call') {
		return
	}

	const { id, method, args } = msg

	try {
		// Resolve the method on the socket object (supports dot-separated paths)
		let target: any = sock
		const path = method.split('.')
		for (const segment of path) {
			if (target === null || target === undefined) {
				throw new Error(`Cannot read property '${segment}' of ${target}`)
			}
			target = target[segment]
		}

		if (typeof target === 'function') {
			const result = await target.apply(
				path.length > 1 ? resolveTarget(sock, path.slice(0, -1)) : sock,
				args || []
			)
			parentPort!.postMessage({ type: 'result', id, result })
		} else {
			// Property access
			parentPort!.postMessage({ type: 'result', id, result: target })
		}
	} catch (err: any) {
		parentPort!.postMessage({
			type: 'result',
			id,
			error: err?.message || String(err),
		})
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

// Signal that the worker is ready
parentPort.postMessage({ type: 'ready' })
