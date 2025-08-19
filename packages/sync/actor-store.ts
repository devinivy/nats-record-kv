import type { KvStore } from './kv-store.ts'

export class ActorStore {
  public kv: KvStore

  constructor(kv: KvStore) {
    this.kv = kv
  }

  async get(did: string): Promise<Actor | null> {
    const raw = await this.kv.get(['actor', did])
    if (raw == null) return raw
    return JSON.parse(raw) as Actor
  }

  async put(did: string, actor: Actor): Promise<Actor> {
    await this.kv.put(['actor', did], JSON.stringify(actor))
    return actor
  }
}

export type Actor = {
  did: string
  pubKey: string | null
  rev: string | null
  dataCid: string | null
  status: ActorStatus | null
  upstreamStatus: ActorStatus | 'unknown' | null
}

export type ActorStatus =
  | 'takendown'
  | 'suspended'
  | 'deleted'
  | 'deactivated'
  | 'desynchronized'
  | 'throttled'

export function createActor(actor: Partial<Actor> & { did: string }): Actor {
  return Object.assign({}, actor, {
    dataCid: null,
    pubKey: null,
    rev: null,
    status: 'desynchronized',
    upstreamStatus: null,
  })
}

export function isActorStatus(status: unknown): status is ActorStatus {
  return (
    status === 'takendown' ||
    status === 'suspended' ||
    status === 'deleted' ||
    status === 'deactivated' ||
    status === 'desynchronized' ||
    status === 'throttled'
  )
}
