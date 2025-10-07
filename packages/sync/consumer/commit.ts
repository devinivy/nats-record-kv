import { MST } from '@atproto/repo'
import { isRepoOpStrict, type Commit } from '../types.ts'
import { syncActorRepo } from './sync.ts'
import {
  getCommit,
  invertOps,
  syncPubKey,
  truncatedCid,
  verifyCommitSig,
  type SyncConsumerContext,
} from './util.ts'

export async function commit(evt: Commit, ctx: SyncConsumerContext) {
  const { actorStore, recordStore } = ctx
  if (!evt.prevData) return // non-sync1.1, skip/warn
  if (!evt.ops.every(isRepoOpStrict)) return // non-sync1.1, skip/warn
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
    await syncActorRepo({ did, blocks: evt.blocks, actor }, ctx)
    return
  }
  const { commit, blockstore } = await getCommit(evt.blocks)
  if (commit.did !== actor.did) {
    return // bad commit
  }
  if (commit.rev <= actor.rev) {
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
  // invert opts on top of covering proof mst
  const mst = MST.load(blockstore, commit.data)
  const dataCid = await invertOps(mst, evt.ops).catch(() => undefined)
  if (!dataCid) {
    return // could not invert ops, indicates a programmer error.
  }
  if (evt.prevData.toString() !== dataCid.toString()) {
    return // event not internally consistent, indicates a programmer error.
  }
  if (actor.dataCid !== dataCid.toString()) {
    // ops inverted but mismatching current state, indicates an operational error: sync.
    if (!actor.status) {
      actor = await actorStore.put(did, { ...actor, status: 'desynchronized' })
    }
    await syncActorRepo({ did, blocks: evt.blocks, actor }, ctx)
    return
  }
  for (const op of evt.ops) {
    // @TODO emit record values
    const [collection, rkey] = op.path.split('/')
    await recordStore.put([did, collection, rkey], {
      rev: commit.rev,
      cid: op.cid ? truncatedCid(op.cid) : null,
    })
  }
  await actorStore.put(did, {
    ...actor,
    rev: commit.rev,
    dataCid: commit.data.toString(),
  })
}
