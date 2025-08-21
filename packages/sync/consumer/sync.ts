import assert from 'node:assert'
import { createActor, type Actor } from '../actor-store.ts'
import type { Sync } from '../types.ts'
import {
  getCommit,
  syncPubKey,
  verifyCommitSig,
  type SyncConsumerContext,
} from './util.ts'

// @TODO abuse counters
// @TODO diff and emit record ops

export async function sync(evt: Sync, ctx: SyncConsumerContext) {
  const actor = await ctx.actorStore.get(evt.did)
  await syncActorRepo({ did: evt.did, blocks: evt.blocks, actor }, ctx)
}

// @NOTE mutates and returns actor
export async function syncActorRepo(
  {
    did,
    blocks,
    actor,
  }: { did: string; actor: Actor | null; blocks: Uint8Array },
  ctx: SyncConsumerContext,
) {
  const { actorStore } = ctx
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
  const { commit } = await getCommit(blocks)
  if (commit.did !== actor.did) {
    return // bad commit
  }
  if (actor.rev && commit.rev <= actor.rev) {
    return // known rev is higher
  }
  // validate commit, and if that fails then sync pubkey and try again
  let valid = await verifyCommitSig(actor, commit)
  if (!valid) {
    const prevPubKey = actor.pubKey
    actor = await syncPubKey(actor, ctx)
    if (actor.pubKey !== prevPubKey) {
      valid = await verifyCommitSig(actor, commit)
    }
  }
  if (!valid) return
  return await actorStore.put(did, {
    ...actor,
    rev: commit.rev,
    dataCid: commit.data.toString(),
    status: actor.status === 'desynchronized' ? null : actor.status,
  })
}
