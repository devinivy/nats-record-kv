import { createActor, isActorStatus } from '../actor-store.ts'
import type { Account } from '../types.ts'
import type { SyncConsumerContext } from './util.ts'

export async function account(evt: Account, ctx: SyncConsumerContext) {
  const { actorStore } = ctx
  const did = evt.did
  const status = evt.active
    ? null
    : isActorStatus(evt.status)
      ? evt.status
      : 'unknown'
  const actor = await actorStore.get(did)
  if (!actor) {
    await actorStore.put(did, createActor({ did, upstreamStatus: status }))
  } else {
    await actorStore.put(did, { ...actor, upstreamStatus: status })
  }
}
