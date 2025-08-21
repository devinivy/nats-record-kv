import type { Identity } from '../types.ts'
import { syncPubKey, type SyncConsumerContext } from './util.ts'

export async function identity(evt: Identity, ctx: SyncConsumerContext) {
  const { actorStore } = ctx
  const actor = await actorStore.get(evt.did)
  if (!actor) return
  await syncPubKey(actor, ctx)
}
