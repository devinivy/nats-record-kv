import assert from 'node:assert'
import type { Did, DidResolver } from '@atproto-labs/did-resolver'
import {
  def,
  MemoryBlockstore,
  MST,
  readCarWithRoot,
  verifyCommitSig,
} from '@atproto/repo'
import type { Actor, ActorStore } from '../actor-store.ts'
import type { RepoOp } from '../types.ts'

// @NOTE mutates and returns actor
export async function validateCommit(
  {
    actor,
    blocks,
  }: {
    actor: Actor
    blocks: Uint8Array
  },
  opts: {
    actorStore: ActorStore
    didResolver: DidResolver<'web' | 'plc'>
  },
) {
  const car = await readCarWithRoot(blocks) // @TODO mark validate CIDs once released
  const blockstore = new MemoryBlockstore(car.blocks)
  const commit = await blockstore.readObj(car.root, def.commit)
  if (commit.did !== actor.did) {
    return // bad
  }
  if (actor.rev && commit.rev <= actor.rev) {
    return // known rev is higher
  }
  let validSig = actor.pubKey
    ? await verifyCommitSig(commit, `did:key:${actor.pubKey}`)
    : false
  if (!validSig) {
    // refresh key and try again
    const prevPubKey = actor.pubKey
    actor = await syncPubKey(actor, opts)
    if (actor.pubKey && actor.pubKey !== prevPubKey) {
      validSig = await verifyCommitSig(commit, `did:key:${actor.pubKey}`)
    }
    if (!validSig) return // bad
  }
  return { actor, commit, blockstore }
}

// @NOTE mutates and returns actor
export async function syncPubKey(
  actor: Actor,
  opts: {
    actorStore: ActorStore
    didResolver: DidResolver<'web' | 'plc'>
  },
) {
  const did = actor.did as Did<'web' | 'plc'>
  const { actorStore, didResolver } = opts
  const resolved = await didResolver.resolve(did).catch(() => undefined)
  const verificationMethod = resolved?.verificationMethod?.find((vm) => {
    return (
      typeof vm !== 'string' &&
      (vm.id === '#atproto' || vm.id === `${did}#atproto`) &&
      vm.type === 'Multikey' &&
      vm.publicKeyMultibase
    )
  })
  if (!verificationMethod) {
    return actor
  }
  assert(
    typeof verificationMethod !== 'string' &&
      verificationMethod?.publicKeyMultibase,
  )
  return await actorStore.put(did, {
    ...actor,
    pubKey: verificationMethod.publicKeyMultibase,
  })
}

export async function invertOps(mst: MST, ops: RepoOp[]) {
  for (const op of ops) {
    if (op.action === 'create') {
      mst = await mst.delete(op.path)
    } else if (op.action === 'update') {
      assert(op.prev, 'prev is required on update op')
      mst = await mst.update(op.path, op.prev)
    } else if (op.action === 'delete') {
      assert(op.prev, 'prev is required on delete op')
      mst = await mst.add(op.path, op.prev)
    }
  }
  return await mst.getPointer()
}
