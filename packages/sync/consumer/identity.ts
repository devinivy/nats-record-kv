import { type DidResolver } from '@atproto-labs/did-resolver'
import { ActorStore } from '../actor-store.ts'
import type { Identity } from '../types.ts'
import { syncPubKey } from './util.ts'

export async function identity(
  evt: Identity,
  opts: {
    actorStore: ActorStore
    didResolver: DidResolver<'web' | 'plc'>
  },
) {
  const { actorStore } = opts
  const actor = await actorStore.get(evt.did)
  if (!actor) return
  await syncPubKey(actor, opts)
}
