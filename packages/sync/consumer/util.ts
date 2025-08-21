import assert from 'node:assert'
import type { Did, DidResolver } from '@atproto-labs/did-resolver'
import {
  def,
  MemoryBlockstore,
  MST,
  readCarWithRoot,
  verifyCommitSig as verifyRepoCommitSig,
  type Commit,
} from '@atproto/repo'
import type { CID } from 'multiformats'
import type { Actor, ActorStore } from '../actor-store.ts'
import type { RecordStore } from '../record-store.ts'
import type { RepoOp } from '../types.ts'

export async function getCommit(blocks: Uint8Array) {
  const car = await readCarWithRoot(blocks) // @TODO mark validate CIDs once released
  const blockstore = new MemoryBlockstore(car.blocks)
  const commit = await blockstore.readObj(car.root, def.commit)
  return { commit, blockstore }
}

export async function verifyCommitSig(actor: Actor, commit: Commit) {
  if (!actor.pubKey) return false
  return verifyRepoCommitSig(commit, `did:key:${actor.pubKey}`)
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
  if (!verificationMethod || actor.pubKey === verificationMethod) {
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

export function truncatedCid(cid: CID) {
  return cid.toString().slice(-8)
}

export type SyncConsumerContext = {
  actorStore: ActorStore
  recordStore: RecordStore
  didResolver: DidResolver<'web' | 'plc'>
}

export type HexChar =
  | '0'
  | '1'
  | '2'
  | '3'
  | '4'
  | '5'
  | '6'
  | '7'
  | '8'
  | '9'
  | 'a'
  | 'b'
  | 'c'
  | 'd'
  | 'e'
  | 'f'
