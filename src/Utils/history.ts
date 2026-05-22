import { AxiosRequestConfig } from 'axios'
import { promisify } from 'util'
import { inflate } from 'zlib'
import { proto } from '../../WAProto'
import { Chat, Contact, WAMessageStubType } from '../Types'
import { isJidUser } from '../WABinary'
import { toNumber } from './generics'
import { normalizeMessageContent } from './messages'
import { downloadContentFromMessage } from './messages-media'
import { AuthenticationState } from '../Types'
import caches from './cache-utils'

const inflatePromise = promisify(inflate)

export const downloadHistory = async (msg: proto.Message.IHistorySyncNotification, options: AxiosRequestConfig<{}>) => {
	const stream = await downloadContentFromMessage(msg, 'md-msg-hist', { options })
	const bufferArray: Buffer[] = []
	for await (const chunk of stream) {
		bufferArray.push(chunk)
	}

	let buffer: Buffer = Buffer.concat(bufferArray)

	// decompress buffer
	buffer = await inflatePromise(buffer)

	const syncData = proto.HistorySync.decode(buffer)
	return syncData
}

export const processHistoryMessage = (item: proto.IHistorySync, authState:  AuthenticationState) => {

	const messages: proto.IWebMessageInfo[] = []
	const contacts: Contact[] = []
	const chats: Chat[] = []

	switch (item.syncType) {
		case proto.HistorySync.HistorySyncType.INITIAL_BOOTSTRAP:
		case proto.HistorySync.HistorySyncType.RECENT:
		case proto.HistorySync.HistorySyncType.FULL:
		case proto.HistorySync.HistorySyncType.ON_DEMAND:

		// Extract LID-PN mappings for all sync types
	for (const m of item.phoneNumberToLidMappings || []) {
		if (m.lidJid && m.pnJid) {
			caches.lidCache.set(m.pnJid, m.lidJid)
		}
	}
			for (const chat of item.conversations! as Chat[]) {
				contacts.push({
					id: chat.id,
					name: chat.displayName || chat.name || chat.username || undefined,
					lid: chat.lidJid || undefined,
					jid: isJidUser(chat.id) ? chat.id : undefined
				})

				const toNumberSafe = (v) => v?.toNumber ? v.toNumber() : v;
				const tctokenLabel: string = chat.lidJid || chat.id

			if (chat.tcToken) {
				 authState.keys.set({
					'contacts-tc-token': {
						[tctokenLabel]: {
							token: Buffer.from(chat.tcToken),
							timestamp: String(toNumberSafe(chat.tcTokenTimestamp)),
							senderTimestamp: toNumberSafe(chat.tcTokenSenderTimestamp)
						}
					}
				})
			}
            

				const msgs = chat.messages || []
				delete chat.messages

				for (const item of msgs) {
					const message = item.message!
					messages.push(message)

					if (!chat.messages?.length) {
						// keep only the most recent message in the chat array
						chat.messages = [{ message }]
					}

					if (!message.key.fromMe && !chat.lastMessageRecvTimestamp) {
						chat.lastMessageRecvTimestamp = toNumber(message.messageTimestamp)
					}

					if (
						(message.messageStubType === WAMessageStubType.BIZ_PRIVACY_MODE_TO_BSP ||
							message.messageStubType === WAMessageStubType.BIZ_PRIVACY_MODE_TO_FB) &&
						message.messageStubParameters?.[0]
					) {
						contacts.push({
							id: message.key.participant || message.key.remoteJid!,
							verifiedName: message.messageStubParameters?.[0]
						})
					}
				}

				chats.push({ ...chat })
			}

			break
		case proto.HistorySync.HistorySyncType.PUSH_NAME:
			for (const c of item.pushnames!) {
				contacts.push({ id: c.id!, notify: c.pushname! })
			}

			break
	}

	return {
		chats,
		contacts,
		messages,
		syncType: item.syncType,
		progress: item.progress
	}
}

export const downloadAndProcessHistorySyncNotification = async (
	msg: proto.Message.IHistorySyncNotification,
	options: AxiosRequestConfig<{}>,
	authstate: AuthenticationState
) => {
	const historyMsg = await downloadHistory(msg, options)
	return processHistoryMessage(historyMsg, authstate)
}

export const getHistoryMsg = (message: proto.IMessage) => {
	const normalizedContent = !!message ? normalizeMessageContent(message) : undefined
	const anyHistoryMsg = normalizedContent?.protocolMessage?.historySyncNotification

	return anyHistoryMsg
}
