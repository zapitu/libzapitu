import NodeCache from '@cacheable/node-cache'

const caches = {
	lidCache: new NodeCache<string>({ stdTTL: 3600, checkperiod: 3600 })
}
export default caches
