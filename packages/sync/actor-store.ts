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

  async put(did: string, actor: Actor): Promise<void> {
    await this.kv.put(['actor', did], JSON.stringify(actor))
  }
}

export type Actor = {
  did: string
  rev: string
  status:
    | null // active
    | 'takendown'
    | 'suspended'
    | 'deleted'
    | 'deactivated'
    | 'desynchronized'
    | 'throttled'
}
