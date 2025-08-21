import { createActor } from '../actor-store.ts'
import type { Identity } from '../types.ts'
import { syncPubKey, type SyncConsumerContext } from './util.ts'

export async function identity(evt: Identity, ctx: SyncConsumerContext) {
  const { actorStore } = ctx
  const did = evt.did
  let actor = await actorStore.get(did)
  if (!actor) {
    actor = await actorStore.put(did, createActor({ did }))
  }
  await syncPubKey(actor, ctx)
}
