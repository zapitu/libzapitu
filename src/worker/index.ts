/**
 * Worker entry point — encapsulated mode.
 *
 * Import from here to run makeWASocket inside a child worker_threads
 * process instead of the main thread.  Multiple sockets are automatically
 * distributed across a pool of workers (default: CPU count).
 *
 * @example
 *   import makeWASocket from 'libzapitu-rf/worker'
 *   const sock = makeWASocket({ ... })
 *   sock.ev.on('messages.upsert', ...)
 *   await sock.sendMessage(...)
 *
 *   // Pool stats (available on every socket):
 *   console.log(sock.pool) // { size, active, idle, totalSockets }
 *
 *   // Global pool control:
 *   import { resizePool, drainPool, getStats } from 'libzapitu-rf/worker'
 *   await resizePool(4)   // scale to 4 workers
 *   await drainPool()     // wait for all sockets to close, then terminate
 */
import makeWASocket, { getStats, resizePool, drainPool } from './proxy'

export * from '../Types'
export * from '../Utils'
export * from '../Defaults'
export * from '../WABinary'
export * from '../WAM'
export * from '../WAUSync'

export type WASocket = ReturnType<typeof makeWASocket>
export { makeWASocket, getStats, resizePool, drainPool }
export default makeWASocket
