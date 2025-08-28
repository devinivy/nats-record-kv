import assert from 'node:assert'
import { DatabaseSync, StatementSync } from 'node:sqlite'

export type KvKey = string[]
export type KvValue = string

const NUL = '\x00'
const HI_BUF = Buffer.from('\xFF')

export interface KvStore {
  get(key: KvKey): Promise<KvValue | null>
  put(key: KvKey, value: KvValue): Promise<void>
  del(key: KvKey): Promise<void>
  scan(prefix: KvKey): AsyncIterable<[KvKey, KvValue]>
}

export class SqliteKvStore implements KvStore {
  public db: DatabaseSync

  constructor(db = new DatabaseSync(':memory:')) {
    this.db = db
    this.db.exec(
      `CREATE TABLE IF NOT EXISTS kv_store (
        key BLOB PRIMARY KEY,
        value TEXT NOT NULL
      ) STRICT`,
    )
  }

  private getQuery?: StatementSync
  async get(key: KvKey): Promise<KvValue | null> {
    const getQuery = (this.getQuery ??= this.db.prepare(
      'SELECT value FROM kv_store WHERE key = :key',
    ))
    const result = getQuery.get({ key: this.key(key) })
    if (!result) return null
    return result.value as string
  }

  private putQuery?: StatementSync
  async put(key: KvKey, value: KvValue): Promise<void> {
    const putQuery = (this.putQuery ??= this.db.prepare(
      `INSERT INTO kv_store (key, value) VALUES (:key, :value)
        ON CONFLICT (key) DO UPDATE SET value = :value`,
    ))
    putQuery.run({ key: this.key(key), value })
  }

  private delQuery?: StatementSync
  async del(key: KvKey): Promise<void> {
    const delQuery = (this.delQuery ??= this.db.prepare(
      'DELETE FROM kv_store WHERE key = :key',
    ))
    delQuery.run({ key: this.key(key) })
  }

  private scanQuery?: StatementSync
  private scanQueryNext?: StatementSync
  async *scan(prefix: KvKey) {
    const scanQuery = (this.scanQuery ??= this.db.prepare(
      `SELECT key, value FROM kv_store
        WHERE key >= :start AND key < :end
        ORDER BY key ASC
        LIMIT 1000`,
    ))
    const scanQueryNext = (this.scanQueryNext ??= this.db.prepare(
      `SELECT key, value FROM kv_store
        WHERE key >= :start AND key < :end AND key > :cursor
        ORDER BY key ASC
        LIMIT 1000`,
    ))
    const start = this.key(prefix)
    const end = Buffer.concat([start, HI_BUF])
    let cursor: Uint8Array | undefined
    do {
      const items = cursor
        ? scanQueryNext.all({ start, end, cursor })
        : scanQuery.all({ start, end })
      for (const item of items) {
        // console.log(item)
        const key = this.parseKey(item.key as Uint8Array)
        const value = item.value as string
        yield [key, value] satisfies [KvKey, KvValue]
      }
      cursor = items.at(-1)?.key as Uint8Array | undefined
    } while (cursor)
  }

  private key(key: KvKey) {
    // e.g. ['namespace', 'key-a', 'key-b'] -> namespace\x00key-a\x00key-b\x00
    assert(key.every((part) => part && !part.includes(NUL)))
    return Buffer.from(key.join(NUL) + NUL)
  }

  private parseKey(key: Uint8Array): KvKey {
    // e.g.  namespace\x00key-a\x00key-b\x00 -> ['namespace', 'key-a', 'key-b']
    const parts = Buffer.from(key).toString().split(NUL)
    assert(parts.pop() === '')
    return parts
  }
}
