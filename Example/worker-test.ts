/**
 * Worker-encapsulated libzapitu test.
 *
 * Scans QR code, connects, sends a message to a target number,
 * and waits for message ack + replies.
 *
 * Usage:
 *   npx ts-node Example/worker-test.ts <target_number>
 *
 *   Example:
 *   npx ts-node Example/worker-test.ts 5511999999999
 *
 * With pairing code:
 *   npx ts-node Example/worker-test.ts 5511999999999 --use-pairing-code
 */
import makeWASocket, {
	DisconnectReason,
	fetchLatestBaileysVersion,
	makeCacheableSignalKeyStore,
	useMultiFileAuthState
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
const logger = P({ timestamp: () => `,"time":"${new Date().toJSON()}"` }, P.destination('./wa-worker-logs.txt'))
logger.level = 'trace'

const usePairingCode = process.argv.includes('--use-pairing-code')
const targetNumber = process.argv[2]?.replace(/\D/g, '')

if (!targetNumber) {
	console.error('Usage: npx ts-node Example/worker-test.ts <target_number>')
	console.error('Example: npx ts-node Example/worker-test.ts 5511999999999')
	process.exit(1)
}

const targetJid = `${targetNumber}@s.whatsapp.net`
console.log(`🎯 Target: ${targetJid}`)

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
			keys: makeCacheableSignalKeyStore(state.keys, logger)
		},
		msgRetryCounterCache,
		generateHighQualityLinkPreview: false
	})

	// ---- Pool stats (worker-specific) ----
	// console.log('worker pool stats:', sock.pool)

	// ---- Pairing code flow ----
	if (usePairingCode && !sock.authState.creds.registered) {
		const phoneNumber = await question('Please enter your phone number:\n')
		const code = await sock.requestPairingCode(phoneNumber)
		console.log(`Pairing code: ${code}`)
	}

	// ---- Track sent message for ack correlation ----
	let sentMsgKey: any = null
	let connected = false

	// ---- Event handling via ev.on() ----
	sock.ev.on('connection.update', (update: any) => {
		const { connection, lastDisconnect, qr } = update

		// Print QR code to terminal
		if (qr) {
			console.log('\n📱 Scan the QR code below:\n')
			qrcode.generate(qr, { small: true })
		}

		if (connection === 'open') {
			connected = true
			console.log('✅ Connected successfully!')
			// sock.pool && console.log('pool stats:', sock.pool)

			// Send message to target after 60s delay
			console.log('⏳ Waiting 60s before sending message...')
			setTimeout(() => sendTestMessage(sock), 60_000)
		}

		if (connection === 'close') {
			connected = false
			const statusCode = (lastDisconnect?.error as Boom)?.output?.statusCode
			const shouldReconnect = statusCode !== DisconnectReason.loggedOut

			console.log(`🔌 Connection closed. Reason: ${statusCode}. Reconnecting: ${shouldReconnect}`)

			if (shouldReconnect) {
				startSock()
			} else {
				console.log('Logged out. Exiting.')
				process.exit(0)
			}
		}
	})

	sock.ev.on('creds.update', async () => {
		await saveCreds()
	})

	// ---- Message receipt (ack) ----
	sock.ev.on('message-receipt.update', (updates: any[]) => {
		for (const update of updates) {
			const { key, receipt } = update
			// Check if this ack is for our sent message
			if (sentMsgKey && key.id === sentMsgKey.id && key.remoteJid === sentMsgKey.remoteJid) {
				const statusLabels: Record<string, string> = {
					server: '📤 Server',
					delivery: '✅ Delivered',
					read: '👁️ Read',
					played: '▶️ Played'
				}
				const label = statusLabels[receipt.type] || receipt.type
				console.log(`${label} ack for message ${key.id}`)
			}
		}
	})

	// ---- Incoming messages (replies) ----
	sock.ev.on('messages.upsert', ({ messages, type }: any) => {
		if (type === 'notify') {
			for (const msg of messages) {
				// Only log messages from our target
				const remoteJid = msg.key?.remoteJid
				if (remoteJid === targetJid && !msg.key?.fromMe) {
					const text =
						msg.message?.conversation ||
						msg.message?.extendedTextMessage?.text ||
						msg.message?.imageMessage?.caption ||
						'(media/unknown)'
					console.log(`💬 Reply from ${remoteJid}: ${text}`)
				}
			}
		}
	})

	// ---- Send test message ----
	async function sendTestMessage(sock: any) {
		try {
			const messageText = `Hello from libzapitu worker! Sent at ${new Date().toISOString()}`
			console.log(`\n📨 Sending to ${targetJid}: "${messageText}"`)

			const result = await sock.sendMessage(targetJid, { text: messageText })
			sentMsgKey = result?.key
			console.log(`📨 Message sent! ID: ${sentMsgKey?.id}`)
			console.log('⏳ Waiting for ack and replies... (Ctrl+C to exit)\n')
		} catch (err: any) {
			console.error('❌ Failed to send message:', err.message)
		}
	}
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------
startSock().catch(err => {
	console.error('Fatal error:', err)
	process.exit(1)
})
