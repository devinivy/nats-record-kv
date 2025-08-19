import { DatabaseSync, StatementSync } from 'node:sqlite'

export type KvKey = string[]
export type KvValue = string

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
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS kv_store (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      ) STRICT;
    `)
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
        ON CONFLICT (key) DO UPDATE SET value = :value;`,
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
        WHERE key >= :prefix AND key < (:prefix || X'FFFF')
        ORDER BY key ASC
        LIMIT 1000`,
    ))
    const scanQueryNext = (this.scanQueryNext ??= this.db.prepare(
      `SELECT key, value FROM kv_store
        WHERE key >= :prefix AND key < (:prefix || X'FFFF') AND key > :cursor
        ORDER BY key ASC
        LIMIT 1000`,
    ))
    let cursor: string | undefined
    do {
      const items = cursor
        ? scanQueryNext.all({ prefix: this.prefix(prefix), cursor })
        : scanQuery.all({ prefix: this.prefix(prefix) })
      for (const item of items) {
        const key = this.parseKey(item.key as string)
        const value = item.value as string
        yield [key, value] satisfies [KvKey, KvValue]
      }
      cursor = items.at(-1)?.key as string | undefined
    } while (cursor)
  }

  private key(key: KvKey) {
    // e.g. ['namespace', 'key-a', 'key-b'] -> "namespace","key-a","key-b"
    return JSON.stringify(key).slice(1, -1)
  }

  private parseKey(key: string): KvKey {
    // e.g. "namespace","key-a","key-b" -> ['namespace', 'key-a', 'key-b']
    return JSON.parse(`[${key}]`) as KvKey
  }

  private prefix(key: KvKey) {
    // e.g ['namespace', 'key-a'] -> "namespace","key-a",
    return this.key(key) + ','
  }
}
