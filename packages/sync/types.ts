import { CID } from 'multiformats/cid'

const id = 'com.atproto.sync.subscribeRepos'

export type SubscribeReposEvent =
  | $Typed<Commit>
  | $Typed<Sync>
  | $Typed<Identity>
  | $Typed<Account>
  | $Typed<Info>

/** Represents an update of repository state. Note that empty commits are allowed, which include no repo data changes, but an update to rev and signature. */
export interface Commit {
  $type?: 'com.atproto.sync.subscribeRepos#commit'
  seq: number
  repo: string
  commit: CID
  rev: string
  blocks: Uint8Array
  ops: RepoOp[]
  prevData?: CID
  time: string
}

export function isCommit<V>(v: V) {
  return is$typed(v, id, 'commit')
}

/** Updates the repo to a new state, without necessarily including that state on the firehose. Used to recover from broken commit streams, data loss incidents, or in situations where upstream host does not know recent state of the repository. */
export interface Sync {
  $type?: 'com.atproto.sync.subscribeRepos#sync'
  seq: number
  did: string
  blocks: Uint8Array
  rev: string
  time: string
}

export function isSync<V>(v: V) {
  return is$typed(v, id, 'sync')
}

/** Represents a change to an account's identity. Could be an updated handle, signing key, or pds hosting endpoint. Serves as a prod to all downstream services to refresh their identity cache. */
export interface Identity {
  $type?: 'com.atproto.sync.subscribeRepos#identity'
  seq: number
  did: string
  time: string
  /** The current handle for the account, or 'handle.invalid' if validation fails. This field is optional, might have been validated or passed-through from an upstream source. Semantics and behaviors for PDS vs Relay may evolve in the future; see atproto specs for more details. */
  handle?: string
}

export function isIdentity<V>(v: V) {
  return is$typed(v, id, 'identity')
}

/** Represents a change to an account's status on a host (eg, PDS or Relay). The semantics of this event are that the status is at the host which emitted the event, not necessarily that at the currently active PDS. Eg, a Relay takedown would emit a takedown with active=false, even if the PDS is still active. */
export interface Account {
  $type?: 'com.atproto.sync.subscribeRepos#account'
  seq: number
  did: string
  time: string
  /** Indicates that the account has a repository which can be fetched from the host that emitted this event. */
  active: boolean
  /** If active=false, this optional field indicates a reason for why the account is not active. */
  status?:
    | 'takendown'
    | 'suspended'
    | 'deleted'
    | 'deactivated'
    | 'desynchronized'
    | 'throttled'
    | (string & {})
}

export function isAccount<V>(v: V) {
  return is$typed(v, id, 'account')
}

export interface Info {
  $type?: 'com.atproto.sync.subscribeRepos#info'
  name: 'OutdatedCursor' | (string & {})
  message?: string
}

export function isInfo<V>(v: V) {
  return is$typed(v, id, 'info')
}

/** A repo operation, ie a mutation of a single record. */
export interface RepoOp {
  $type?: 'com.atproto.sync.subscribeRepos#repoOp'
  action: 'create' | 'update' | 'delete' | (string & {})
  path: string
  cid: CID | null
  prev?: CID
}

type $Typed<V, T extends string = string> = V & { $type: T }

type $Type<Id extends string, Hash extends string> = Hash extends 'main'
  ? Id
  : `${Id}#${Hash}`

type $TypedObject<V, Id extends string, Hash extends string> = V extends {
  $type: $Type<Id, Hash>
}
  ? V
  : V extends { $type?: string }
    ? V extends { $type?: infer T extends $Type<Id, Hash> }
      ? V & { $type: T }
      : never
    : V & { $type: $Type<Id, Hash> }

function is$typed<V, Id extends string, Hash extends string>(
  v: V,
  id: Id,
  hash: Hash,
): v is $TypedObject<V, Id, Hash> {
  return is$type(v?.['$type'], id, hash)
}

function is$type<Id extends string, Hash extends string>(
  $type: unknown,
  id: Id,
  hash: Hash,
): $type is $Type<Id, Hash> {
  return hash === 'main' ? $type === id : $type === `${id}#${hash}`
}
