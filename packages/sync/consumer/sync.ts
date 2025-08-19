import assert from 'node:assert'
import type { DidResolver } from '@atproto-labs/did-resolver'
import { ActorStore, createActor, type Actor } from '../actor-store.ts'
import type { Sync } from '../types.ts'
import { validateCommit } from './util.ts'

// @TODO actually diff and emit record ops
// @TODO handle abuse

export async function sync(
  evt: Sync,
  opts: {
    actorStore: ActorStore
    didResolver: DidResolver<'web' | 'plc'>
  },
) {
  const actor = await opts.actorStore.get(evt.did)
  await syncActorRepo(
    {
      did: evt.did,
      blocks: evt.blocks,
      actor,
    },
    opts,
  )
}

// @NOTE mutates and returns actor
export async function syncActorRepo(
  {
    did,
    blocks,
    actor,
  }: { did: string; actor: Actor | null; blocks: Uint8Array },
  opts: {
    actorStore: ActorStore
    didResolver: DidResolver<'web' | 'plc'>
  },
) {
  const { actorStore } = opts
  assert(!actor || actor.did === did)
  if (!actor) {
    actor = await actorStore.put(did, createActor({ did }))
  }
  if (actor.upstreamStatus) {
    return // inactive upstream
  }
  if (actor.status === 'deleted' || actor.status === 'throttled') {
    return // local status set to stop processing
  }
  const validated = await validateCommit({ actor, blocks }, opts)
  if (!validated) return
  actor = validated.actor
  return await actorStore.put(did, {
    ...actor,
    rev: validated.commit.rev,
    dataCid: validated.commit.data.toString(),
    status: actor.status === 'desynchronized' ? null : actor.status,
  })
}
