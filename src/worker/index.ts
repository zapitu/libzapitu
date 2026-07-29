/**
 * Worker entry point — encapsulated mode.
 *
 * Import from here to run makeWASocket inside a child worker_threads
 * process instead of the main thread.
 *
 * @example
 *   import makeWASocket from 'libzapitu-rf/worker'
 *   const sock = makeWASocket({ ... })
 *   sock.ev.on('messages.upsert', ...)
 *   await sock.sendMessage(...)
 */
import makeWASocket from './proxy'

export * from '../Types'
export * from '../Utils'
export * from '../Defaults'
export * from '../WABinary'
export * from '../WAM'
export * from '../WAUSync'

export type WASocket = ReturnType<typeof makeWASocket>
export { makeWASocket }
export default makeWASocket
