/**
 * Minimal test for the worker-encapsulated libzapitu.
 *
 * Starts a connection, prints the QR code to the terminal,
 * and logs connection state changes.
 *
 * Usage:
 *   npx ts-node Example/worker-test.ts
 *
 * With pairing code:
 *   npx ts-node Example/worker-test.ts --use-pairing-code
 */
import makeWASocket, {
	DisconnectReason,
	fetchLatestBaileysVersion,
	makeCacheableSignalKeyStore,
	useMultiFileAuthState,
} from '../src/worker'
import { Boom } from '@hapi/boom'
import NodeCache from '@cacheable/node-cache'
import type { CacheStore } from '../src/Types'
import P from 'pino'
import readline from 'readline'
import qrcode from 'qrcode-terminal'

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------
const logger = P(
	{ timestamp: () => `,"time":"${new Date().toJSON()}"` },
	P.destination('./wa-worker-logs.txt')
)
logger.level = 'trace'

const usePairingCode = process.argv.includes('--use-pairing-code')
const msgRetryCounterCache = new NodeCache() as unknown as CacheStore

const rl = readline.createInterface({ input: process.stdin, output: process.stdout })
const question = (text: string) => new Promise<string>(resolve => rl.question(text, resolve))

// ---------------------------------------------------------------------------
// Start socket
// ---------------------------------------------------------------------------
async function startSock() {
	const { state, saveCreds } = await useMultiFileAuthState('baileys_auth_info_worker')

	const { version, isLatest } = await fetchLatestBaileysVersion()
	console.log(`using WA v${version.join('.')}, isLatest: ${isLatest}`)

	const sock = makeWASocket({
		version,
		logger,
		auth: {
			creds: state.creds,
			keys: makeCacheableSignalKeyStore(state.keys, logger),
		},
		msgRetryCounterCache,
		generateHighQualityLinkPreview: false,
	})

	// ---- Pool stats (worker-specific) ----
	console.log('worker pool stats:', sock.pool)

	// ---- Pairing code flow ----
	if (usePairingCode && !sock.authState.creds.registered) {
		const phoneNumber = await question('Please enter your phone number:\n')
		const code = await sock.requestPairingCode(phoneNumber)
		console.log(`Pairing code: ${code}`)
	}

	// ---- Event handling ----
	sock.ev.process(async events => {
		// Connection state changes
		if (events['connection.update']) {
			const update = events['connection.update']
			const { connection, lastDisconnect, qr } = update

			// Print QR code to terminal
			if (qr) {
				console.log('\n📱 Scan the QR code below:\n')
				qrcode.generate(qr, { small: true })
			}

			if (connection === 'open') {
				console.log('✅ Connected successfully!')
				console.log('pool stats:', sock.pool)
			}

			if (connection === 'close') {
				const statusCode = (lastDisconnect?.error as Boom)?.output?.statusCode
				const shouldReconnect = statusCode !== DisconnectReason.loggedOut

				console.log(
					`🔌 Connection closed. Reason: ${statusCode}. Reconnecting: ${shouldReconnect}`
				)

				if (shouldReconnect) {
					startSock()
				} else {
					console.log('Logged out. Exiting.')
					process.exit(0)
				}
			}
		}

		// Save credentials
		if (events['creds.update']) {
			await saveCreds()
		}

		// Log received messages (just the count)
		if (events['messages.upsert']) {
			const { messages, type } = events['messages.upsert']
			console.log(`📩 Received ${messages.length} message(s) [type: ${type}]`)
		}
	})
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------
startSock().catch(err => {
	console.error('Fatal error:', err)
	process.exit(1)
})
