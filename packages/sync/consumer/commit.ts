import type { DidResolver } from '@atproto-labs/did-resolver'
import { MST } from '@atproto/repo'
import { type ActorStore } from '../actor-store.ts'
import type { Commit } from '../types.ts'
import { syncActorRepo } from './sync.ts'
import { invertOps, validateCommit } from './util.ts'

export async function commit(
  evt: Commit,
  opts: {
    actorStore: ActorStore
    didResolver: DidResolver<'web' | 'plc'>
  },
) {
  const { actorStore } = opts
  if (!evt.prevData) return // non-sync1.1, skip/warn
  const did = evt.repo
  let actor = await actorStore.get(did)
  if (actor?.upstreamStatus) {
    return // inactive upstream
  }
  if (actor?.status === 'deleted' || actor?.status === 'throttled') {
    return // local status set to stop processing
  }
  if (
    !actor ||
    !actor.rev ||
    !actor.dataCid ||
    actor.status === 'desynchronized'
  ) {
    await syncActorRepo({ did, blocks: evt.blocks, actor }, opts)
    return
  }
  const validated = await validateCommit({ actor, blocks: evt.blocks }, opts)
  if (!validated) return
  actor = validated.actor
  const mst = MST.load(validated.blockstore, validated.commit.data)
  const dataCid = await invertOps(mst, evt.ops).catch(() => undefined)
  if (!dataCid) {
    // could not invert ops, indicates a programmer error: bail.
    return
  }
  if (actor.dataCid !== dataCid.toString()) {
    // ops inverted but mismatching current state, indicates an operational error: sync.
    if (!actor.status) {
      actor = await actorStore.put(did, { ...actor, status: 'desynchronized' })
    }
    await syncActorRepo({ did, blocks: evt.blocks, actor }, opts)
    return
  }
  await actorStore.put(did, {
    ...actor,
    rev: validated.commit.rev,
    dataCid: validated.commit.data.toString(),
  })
}
