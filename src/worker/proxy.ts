/**
 * Worker proxy — parent-side façade.
 *
 * Spawns a worker_threads child that runs the real makeWASocket and
 * proxies every method call and event back and forth transparently.
 *
 * Usage (encapsulated mode):
 *   import makeWASocket from 'libzapitu-rf/worker'
 *
 * All non-serializable config callbacks (getMessage, shouldIgnoreJid, …)
 * are automatically forwarded from the worker to the parent, executed
 * here, and the result is sent back.
 */
import { Worker } from 'worker_threads'
import { EventEmitter } from 'events'
import { join } from 'path'
import type { UserFacingSocketConfig } from '../Types'

// ---------------------------------------------------------------------------
// Keys whose values are functions that cannot be serialized across
// the worker boundary. The proxy will replace them with stubs on the
// worker side and invoke the real callbacks here on the parent side.
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
let _reqId = 0
const nextReqId = (): number => ++_reqId

// ---------------------------------------------------------------------------
// makeWASocket (proxy)
// ---------------------------------------------------------------------------
const makeWASocket = (config: UserFacingSocketConfig) => {
	// ---- Extract non-serializable callbacks from config ------------------
	const callbackStore: Partial<Record<CallbackKey, Function>> = {}
	const serializableConfig: any = { ...config }

	for (const k of CALLBACK_CONFIG_KEYS) {
		if (typeof (serializableConfig as any)[k] === 'function') {
			callbackStore[k as CallbackKey] = (serializableConfig as any)[k]
			// Replace with a sentinel so the worker creates a forwarding stub.
			// The child.ts code checks for function type to replace, so we
			// keep a dummy function that the child will detect and override.
			;(serializableConfig as any)[k] = '__proxy_callback__'
		}
	}

	// ---- Spawn worker ----------------------------------------------------
	const workerScript = join(__dirname, 'child.js')
	const worker = new Worker(workerScript, {
		workerData: { config: serializableConfig },
	})

	// ---- Public event emitter --------------------------------------------
	const ev = new EventEmitter()

	// ---- Pending RPC calls -----------------------------------------------
	const pending = new Map<
		number,
		{ resolve: (v: any) => void; reject: (e: Error) => void }
	>()

	// ---- Handle messages from worker -------------------------------------
	worker.on('message', (msg: any) => {
		if (!msg || typeof msg !== 'object') return

		switch (msg.type) {
			// --- Forwarded event from the socket ---
			case 'event':
				ev.emit(msg.event, msg.data)
				break

			// --- RPC method/property result ---
			case 'result':
				{
					const p = pending.get(msg.id)
					if (p) {
						pending.delete(msg.id)
						if (msg.error) {
							p.reject(new Error(msg.error))
						} else {
							p.resolve(msg.result)
						}
					}
				}
				break

			// --- Callback invocation forwarded from worker ---
			case 'callback-call':
				handleCallbackCall(msg)
				break
		}
	})

	// ---- Handle forwarded config callbacks -------------------------------
	async function handleCallbackCall(msg: {
		id: number
		key: CallbackKey
		args: unknown[]
	}) {
		const { id, key, args } = msg
		const fn = callbackStore[key]
		if (!fn) {
			return worker.postMessage({
				type: 'callback-result',
				id,
				error: `No callback registered for "${key}"`,
			})
		}
		try {
			const result = await fn(...args)
			worker.postMessage({ type: 'callback-result', id, result })
		} catch (err: any) {
			worker.postMessage({
				type: 'callback-result',
				id,
				error: err?.message || String(err),
			})
		}
	}

	// ---- RPC helper: call a method on the worker -------------------------
	const rpcCall = (method: string, args: unknown[]): Promise<unknown> => {
		const id = nextReqId()
		return new Promise((resolve, reject) => {
			pending.set(id, { resolve, reject })
			worker.postMessage({ type: 'call', id, method, args })
		})
	}

	// ---- Build the proxy object ------------------------------------------
	// We use a Proxy so that method calls are transparently forwarded to
	// the worker thread. The event emitter `ev` is handled locally.
	//
	// NOTE: Because JavaScript Proxy `get` traps cannot distinguish a
	// property read (`x.foo`) from a method call (`x.foo()`), every
	// property access returns an async RPC function. Synchronous property
	// reads (except `ev`) will return a Promise instead of the raw value.
	// If you need to read a property value synchronously, await it:
	//   const user = await sock.user
	// This is an acceptable trade-off for a worker-encapsulated socket.
	const socketProxy = new Proxy(
		{ ev },
		{
			get(_target, prop: string) {
				if (prop === 'ev') return ev
				if (prop === 'then') return undefined // prevent Promise-like confusion

				// end() must terminate the worker after the socket closes
				if (prop === 'end') {
					return async (...args: unknown[]) => {
						try { await rpcCall('end', args) } catch (_) { /* ok */ }
						await worker.terminate()
					}
				}

				// logout() should also terminate the worker
				if (prop === 'logout') {
					return async (...args: unknown[]) => {
						try { await rpcCall('logout', args) } catch (_) { /* ok */ }
						await worker.terminate()
					}
				}

				// Return an async function that proxies the method call.
				// For property reads the user must `await sock.property`.
				return (...args: unknown[]) => rpcCall(prop, args)
			},
		}
	) as any

	return socketProxy
}

export default makeWASocket
export { makeWASocket }
