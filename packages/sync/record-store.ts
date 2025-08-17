import type { KvStore } from './kv-store.ts'

export class RecordStore {
  public kv: KvStore

  constructor(kv: KvStore) {
    this.kv = kv
  }

  async get(key: RecordKey): Promise<RecordInfo | null> {
    const raw = await this.kv.get(['record', ...key])
    if (raw == null) return raw
    return JSON.parse(raw) as RecordInfo
  }

  async put(key: RecordKey, record: RecordInfo): Promise<void> {
    await this.kv.put(['record', ...key], JSON.stringify(record))
  }

  async *scanRepo(did: string) {
    for await (const [rawkey, rawvalue] of this.kv.scan(['record', did])) {
      const [_, did, collection, rkey] = rawkey
      const key: RecordKey = [did, collection, rkey]
      const record: RecordInfo = JSON.parse(rawvalue)
      yield [key, record]
    }
  }
}

export type RecordKey = [did: string, collection: string, rkey: string]

export type RecordInfo = {
  rev: string
  cid: string | null
}
