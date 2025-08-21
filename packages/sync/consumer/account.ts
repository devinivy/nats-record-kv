import { createActor, isActorStatus } from '../actor-store.ts'
import type { Account } from '../types.ts'
import type { SyncConsumerContext } from './util.ts'

export async function account(evt: Account, ctx: SyncConsumerContext) {
  const { actorStore } = ctx
  const status = evt.active
    ? null
    : isActorStatus(evt.status)
      ? evt.status
      : 'unknown'
  const actor = await actorStore.get(evt.did)
  if (!actor) {
    await actorStore.put(
      evt.did,
      createActor({ did: evt.did, upstreamStatus: status }),
    )
  } else {
    await actorStore.put(evt.did, {
      ...actor,
      upstreamStatus: status,
    })
  }
}
