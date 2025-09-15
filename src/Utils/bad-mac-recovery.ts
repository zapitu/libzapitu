import { Boom } from '@hapi/boom'
import type { SignalAuthState } from '../Types'
import { jidNormalizedUser } from '../WABinary'
import logger from './logger'

export interface BadMACError {
	jid: string
	type: '1:1' | 'group'
	authorJid?: string
	timestamp: number
	attempt: number
	stackTrace: string
}

/**
 * Gerenciador específico para erros "Bad MAC" do libsignal
 */
export class BadMACRecoveryManager {
	private errorHistory = new Map<string, BadMACError[]>()
	private recoveryAttempts = new Map<string, number>()
	private maxRetries = 3
	private cooldownPeriod = 60000

	/**
	 * Detecta se um erro é especificamente "Bad MAC" do libsignal
	 */
	isBadMACError(error: Error): boolean {
		const msg = error.message?.toLowerCase() || ''
		const stack = error.stack?.toLowerCase() || ''

		return (
			msg.includes('bad mac') ||
			msg.includes('mac error') ||
			(stack.includes('verifymac') && stack.includes('crypto.js')) ||
			(stack.includes('session_cipher.js') && msg.includes('mac'))
		)
	}

	/**
	 * Registra um erro Bad MAC
	 */
	recordBadMACError(jid: string, error: Error, type: '1:1' | 'group', authorJid?: string): BadMACError {
		const normalizedJid = jidNormalizedUser(jid)
		const key = type === 'group' && authorJid ? `${normalizedJid}:${jidNormalizedUser(authorJid)}` : normalizedJid

		const currentAttempts = this.recoveryAttempts.get(key) || 0
		this.recoveryAttempts.set(key, currentAttempts + 1)

		const badMACError: BadMACError = {
			jid: normalizedJid,
			type,
			authorJid: authorJid ? jidNormalizedUser(authorJid) : undefined,
			timestamp: Date.now(),
			attempt: currentAttempts + 1,
			stackTrace: error.stack || ''
		}

		if (!this.errorHistory.has(key)) {
			this.errorHistory.set(key, [])
		}

		this.errorHistory.get(key)!.push(badMACError)

		logger.warn(
			{
				jid: normalizedJid,
				type,
				authorJid,
				attempt: badMACError.attempt,
				error: error.message
			},
			'Bad MAC error recorded'
		)

		return badMACError
	}

	/**
	 * Verifica se deve tentar recuperação automática
	 */
	shouldAttemptRecovery(jid: string, authorJid?: string): boolean {
		const normalizedJid = jidNormalizedUser(jid)
		const key = authorJid ? `${normalizedJid}:${jidNormalizedUser(authorJid)}` : normalizedJid

		const attempts = this.recoveryAttempts.get(key) || 0
		const lastErrors = this.errorHistory.get(key) || []

		if (lastErrors.length > 0) {
			const lastError = lastErrors[lastErrors.length - 1]
			if (Date.now() - lastError.timestamp < this.cooldownPeriod) {
				return attempts < this.maxRetries
			}
		}

		this.recoveryAttempts.set(key, 0)
		return true
	}

	/**
	 * Executa recuperação automática para erro Bad MAC
	 */
	async attemptRecovery(
		jid: string,
		authState: SignalAuthState,
		type: '1:1' | 'group',
		authorJid?: string
	): Promise<boolean> {
		const normalizedJid = jidNormalizedUser(jid)
		const normalizedAuthorJid = authorJid ? jidNormalizedUser(authorJid) : undefined

		if (!this.shouldAttemptRecovery(normalizedJid, normalizedAuthorJid)) {
			logger.warn(
				{
					jid: normalizedJid,
					authorJid: normalizedAuthorJid,
					type
				},
				'Bad MAC recovery skipped - max retries exceeded or in cooldown'
			)
			return false
		}

		try {
			logger.info(
				{
					jid: normalizedJid,
					authorJid: normalizedAuthorJid,
					type
				},
				'Attempting Bad MAC recovery'
			)

			if (type === '1:1') {
				await this.recover1to1Session(normalizedJid, authState)
			} else if (type === 'group' && normalizedAuthorJid) {
				await this.recoverGroupSenderKey(normalizedJid, normalizedAuthorJid, authState)
			}

			logger.info(
				{
					jid: normalizedJid,
					authorJid: normalizedAuthorJid,
					type
				},
				'Bad MAC recovery completed successfully'
			)

			return true
		} catch (recoveryError) {
			logger.error(
				{
					jid: normalizedJid,
					authorJid: normalizedAuthorJid,
					type,
					recoveryError
				},
				'Bad MAC recovery failed'
			)

			return false
		}
	}

	/**
	 * Recupera sessão 1:1 removendo dados corrompidos
	 */
	private async recover1to1Session(jid: string, authState: SignalAuthState): Promise<void> {
		await authState.keys.set({
			session: { [jid]: null }
		})

		logger.debug({ jid }, 'Reset session for Bad MAC recovery')
	}

	/**
	 * Recupera sender key de grupo removendo dados corrompidos
	 */
	private async recoverGroupSenderKey(groupJid: string, authorJid: string, authState: SignalAuthState): Promise<void> {
		const { SenderKeyName } = await import('../Signal/Group/sender-key-name')
		const { jidDecode } = await import('../WABinary')

		const decoded = jidDecode(authorJid)
		if (!decoded) {
			throw new Error(`Invalid JID format: ${authorJid}`)
		}

		const sender = {
			id: decoded.user,
			deviceId: decoded.device || 0,
			toString: () => `${decoded.user}.${decoded.device || 0}`
		}

		const senderKeyName = new SenderKeyName(groupJid, sender)
		const keyId = senderKeyName.toString()

		await authState.keys.set({
			'sender-key': { [keyId]: null }
		})

		logger.debug({ groupJid, authorJid, keyId }, 'Reset sender key for Bad MAC recovery')
	}

	/**
	 * Limpa histórico antigo de erros
	 */
	cleanup(): void {
		const cutoff = Date.now() - this.cooldownPeriod * 10
		let cleaned = 0

		this.errorHistory.forEach((errors, key) => {
			const recentErrors = errors.filter(err => err.timestamp > cutoff)

			if (recentErrors.length === 0) {
				this.errorHistory.delete(key)
				this.recoveryAttempts.delete(key)
				cleaned++
			} else if (recentErrors.length < errors.length) {
				this.errorHistory.set(key, recentErrors)
			}
		})

		if (cleaned > 0) {
			logger.debug({ cleaned }, 'Cleaned up old Bad MAC error history')
		}
	}

	/**
	 * Obtém estatísticas de erros Bad MAC
	 */
	getStats(
		jid?: string,
		authorJid?: string
	): {
		jid?: string
		authorJid?: string
		totalErrors?: number
		recoveryAttempts?: number
		lastError?: number
		canRetry?: boolean
		totalJIDs?: number
		totalAttempts?: number
		activeJids?: number
		healthScore?: number
	} {
		if (jid) {
			const normalizedJid = jidNormalizedUser(jid)
			const key = authorJid ? `${normalizedJid}:${jidNormalizedUser(authorJid)}` : normalizedJid
			const errors = this.errorHistory.get(key) || []
			const attempts = this.recoveryAttempts.get(key) || 0

			return {
				jid: normalizedJid,
				authorJid: authorJid ? jidNormalizedUser(authorJid) : undefined,
				totalErrors: errors.length,
				recoveryAttempts: attempts,
				lastError: errors[errors.length - 1]?.timestamp || 0,
				canRetry: this.shouldAttemptRecovery(normalizedJid, authorJid)
			}
		}

		let totalErrors = 0
		let totalAttempts = 0
		let activeJids = 0

		this.errorHistory.forEach((errors, key) => {
			totalErrors += errors.length
			const attempts = this.recoveryAttempts.get(key) || 0
			totalAttempts += attempts

			if (attempts > 0) {
				activeJids++
			}
		})

		return {
			totalJIDs: this.errorHistory.size,
			totalErrors,
			totalAttempts,
			activeJids,
			healthScore: Math.max(0, 100 - (activeJids / Math.max(1, this.errorHistory.size)) * 100)
		}
	}
}

// Instância global
export const badMACRecovery = new BadMACRecoveryManager()

// Cleanup automático
setInterval(() => {
	badMACRecovery.cleanup()
}, 300000)

/**
 * Função utilitária para lidar com erros Bad MAC de forma automática
 */
export async function handleBadMACError(
	jid: string,
	error: Error,
	authState: SignalAuthState,
	authorJid?: string
): Promise<never> {
	const type = authorJid ? 'group' : '1:1'

	const errorInfo = badMACRecovery.recordBadMACError(jid, error, type, authorJid)

	const recovered = await badMACRecovery.attemptRecovery(jid, authState, type, authorJid)

	const boom = new Boom(`Bad MAC error ${recovered ? 'with automatic recovery' : 'requiring manual intervention'}`, {
		statusCode: 500,
		data: {
			jid: errorInfo.jid,
			type: errorInfo.type,
			authorJid: errorInfo.authorJid,
			attempt: errorInfo.attempt,
			recovered,
			canRetry: badMACRecovery.shouldAttemptRecovery(jid, authorJid),
			stats: badMACRecovery.getStats(jid, authorJid)
		}
	})

	throw boom
}
