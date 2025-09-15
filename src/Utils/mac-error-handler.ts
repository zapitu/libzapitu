import { Boom } from '@hapi/boom'
import logger from './logger'

export interface MACErrorInfo {
	jid: string
	errorType: 'bad_mac' | 'invalid_mac' | 'mac_verification_failed'
	originalError: string
	timestamp: number
	attemptCount: number
}

/**
 * Classe para gerenciar e recuperar erros de MAC
 */
export class MACErrorManager {
	private errorHistory = new Map<string, MACErrorInfo[]>()
	private maxRetries = 3
	private cooldownPeriod = 60000 // 1 minuto
	private cleanupInterval = 300000 // 5 minutos

	constructor() {
		// Limpa histórico antigo periodicamente
		setInterval(() => this.cleanupOldErrors(), this.cleanupInterval)
	}

	/**
	 * Detecta se um erro é relacionado a MAC
	 */
	isMACError(error: Error): boolean {
		const errorMsg = error.message?.toLowerCase() || ''
		const stackTrace = error.stack?.toLowerCase() || ''

		const macPatterns = [
			'bad mac',
			'invalid mac',
			'mac verification failed',
			'mac error',
			'authentication failed',
			'verifymac' // Para capturar erros do libsignal
		]

		return macPatterns.some(pattern => errorMsg.includes(pattern) || stackTrace.includes(pattern))
	}

	/**
	 * Registra um erro de MAC
	 */
	recordMACError(jid: string, error: Error): MACErrorInfo {
		const errorInfo: MACErrorInfo = {
			jid,
			errorType: this.categorizeError(error),
			originalError: error.message,
			timestamp: Date.now(),
			attemptCount: this.getAttemptCount(jid) + 1
		}

		if (!this.errorHistory.has(jid)) {
			this.errorHistory.set(jid, [])
		}

		this.errorHistory.get(jid)!.push(errorInfo)

		logger.warn(
			{
				jid,
				errorType: errorInfo.errorType,
				attemptCount: errorInfo.attemptCount,
				error: error.message
			},
			'MAC error recorded'
		)

		return errorInfo
	}

	/**
	 * Verifica se deve tentar recuperar a sessão
	 */
	shouldAttemptRecovery(jid: string): boolean {
		const history = this.errorHistory.get(jid) || []
		const recentErrors = history.filter(err => Date.now() - err.timestamp < this.cooldownPeriod)

		return recentErrors.length < this.maxRetries
	}

	/**
	 * Obtém recomendações de recuperação
	 */
	getRecoveryRecommendations(jid: string): string[] {
		const attemptCount = this.getAttemptCount(jid)
		const recommendations: string[] = []

		if (attemptCount === 1) {
			recommendations.push('Clear corrupted session data')
			recommendations.push('Wait for new key exchange')
			recommendations.push('Message will be retried automatically')
		} else if (attemptCount === 2) {
			recommendations.push('Force session reset')
			recommendations.push('Restart handshake process')
			recommendations.push('Check network connectivity')
		} else if (attemptCount >= 3) {
			recommendations.push('Persistent MAC error detected')
			recommendations.push('Manual session intervention required')
			recommendations.push('Consider full authentication reset')
			recommendations.push('Contact recipient to reinitiate encryption')
		}

		return recommendations
	}

	/**
	 * Limpa o histórico de erros para um JID
	 */
	clearErrorHistory(jid: string): void {
		this.errorHistory.delete(jid)
		logger.info({ jid }, 'MAC error history cleared')
	}

	/**
	 * Obtém estatísticas de erro
	 */
	getErrorStats(jid?: string): {
		totalErrors?: number
		recentErrors?: number
		lastError?: number
		errorRate?: number
		needsIntervention?: boolean
		totalJIDs?: number
		jidsWithIssues?: number
		healthScore?: number
	} {
		if (jid) {
			const history = this.errorHistory.get(jid) || []
			const recentErrors = history.filter(err => Date.now() - err.timestamp < this.cooldownPeriod)
			return {
				totalErrors: history.length,
				recentErrors: recentErrors.length,
				lastError: history[history.length - 1]?.timestamp || 0,
				errorRate: recentErrors.length / Math.max(1, Math.ceil(this.cooldownPeriod / 60000)), // erros por minuto
				needsIntervention: recentErrors.length >= this.maxRetries
			}
		}

		// Estatísticas globais
		let totalErrors = 0
		let recentErrors = 0
		let jidsWithIssues = 0

		this.errorHistory.forEach(history => {
			const recentForJid = history.filter(err => Date.now() - err.timestamp < this.cooldownPeriod)
			totalErrors += history.length
			recentErrors += recentForJid.length
			if (recentForJid.length >= this.maxRetries) {
				jidsWithIssues++
			}
		})

		return {
			totalJIDs: this.errorHistory.size,
			totalErrors,
			recentErrors,
			jidsWithIssues,
			healthScore: Math.max(0, 100 - (jidsWithIssues / Math.max(1, this.errorHistory.size)) * 100)
		}
	}

	/**
	 * Tenta recuperação automática para um JID específico
	 */
	async attemptAutomaticRecovery(jid: string, sessionResetCallback: () => Promise<void>): Promise<boolean> {
		if (!this.shouldAttemptRecovery(jid)) {
			logger.warn({ jid }, 'Cannot attempt recovery - max retries exceeded')
			return false
		}

		try {
			logger.info({ jid }, 'Attempting automatic MAC error recovery')
			await sessionResetCallback()

			// Limpar alguns erros após recovery bem-sucedida
			const history = this.errorHistory.get(jid) || []
			if (history.length > 1) {
				// Manter apenas o último erro como referência
				this.errorHistory.set(jid, [history[history.length - 1]])
			}

			logger.info({ jid }, 'Automatic MAC error recovery completed successfully')
			return true
		} catch (error) {
			logger.error({ jid, error }, 'Automatic MAC error recovery failed')
			return false
		}
	}

	private categorizeError(error: Error): MACErrorInfo['errorType'] {
		const errorMsg = error.message?.toLowerCase() || ''

		if (errorMsg.includes('bad mac')) {
			return 'bad_mac'
		}

		if (errorMsg.includes('invalid mac')) {
			return 'invalid_mac'
		}

		if (errorMsg.includes('verification failed')) {
			return 'mac_verification_failed'
		}

		return 'bad_mac' // default
	}

	private getAttemptCount(jid: string): number {
		const history = this.errorHistory.get(jid) || []
		return history.filter(err => Date.now() - err.timestamp < this.cooldownPeriod).length
	}

	private cleanupOldErrors(): void {
		const cutoff = Date.now() - this.cooldownPeriod * 10 // 10x cooldown period
		let cleaned = 0

		this.errorHistory.forEach((history, jid) => {
			const filtered = history.filter(err => err.timestamp > cutoff)
			if (filtered.length === 0) {
				this.errorHistory.delete(jid)
				cleaned++
			} else if (filtered.length < history.length) {
				this.errorHistory.set(jid, filtered)
			}
		})

		if (cleaned > 0) {
			logger.debug({ cleaned }, 'Cleaned up old MAC error history')
		}
	}
}

// Instância global do gerenciador
export const macErrorManager = new MACErrorManager()

/**
 * Função utilitária para lidar com erros MAC de forma padronizada
 */
export async function handleMACError(jid: string, error: Error, sessionCleanupFn: () => Promise<void>): Promise<never> {
	const errorInfo = macErrorManager.recordMACError(jid, error)

	if (macErrorManager.shouldAttemptRecovery(jid)) {
		logger.info(
			{
				jid,
				attemptCount: errorInfo.attemptCount,
				recommendations: macErrorManager.getRecoveryRecommendations(jid)
			},
			'Attempting MAC error recovery'
		)

		try {
			await sessionCleanupFn()
			logger.info({ jid }, 'Session cleanup completed for MAC error recovery')
		} catch (cleanupError) {
			logger.error({ jid, cleanupError }, 'Failed to cleanup session during MAC error recovery')
		}
	}

	// Sempre relança o erro com informações adicionais
	throw new Boom(`MAC verification failed for ${jid}: ${error.message}`, {
		statusCode: 500,
		data: {
			jid,
			errorType: errorInfo.errorType,
			attemptCount: errorInfo.attemptCount,
			recommendations: macErrorManager.getRecoveryRecommendations(jid),
			canRetry: macErrorManager.shouldAttemptRecovery(jid)
		}
	})
}
