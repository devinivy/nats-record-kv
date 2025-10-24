import assert from 'node:assert'
import { setImmediate } from 'node:timers/promises'
import { chunkArray } from '@atproto/common'
import { BlockMap, ReadableBlockstore, type CarBlock } from '@atproto/repo'
import { CID } from 'multiformats/cid'
import { DatabaseSync, StatementSync } from 'node:sqlite'

export class SqliteBlockstore extends ReadableBlockstore {
  private db: DatabaseSync

  constructor(db = new DatabaseSync(':memory:')) {
    super()
    this.db = db
    this.db.exec(
      `CREATE TABLE IF NOT EXISTS blockstore (
        cid TEXT PRIMARY KEY,
        bytes BLOB NOT NULL
      ) STRICT`,
    )
  }

  private loadQuery?: StatementSync
  async load(blocks: AsyncIterable<CarBlock>): Promise<void> {
    assert(!this.db.isTransaction, 'db is in transaction, likely loading')
    const loadQuery = (this.loadQuery ??= this.db.prepare(
      'INSERT INTO blockstore (cid, bytes) VALUES (?, ?)',
    ))
    this.db.exec('BEGIN TRANSACTION')
    for await (const block of blocks) {
      loadQuery.run(block.cid.toString(), block.bytes)
    }
    this.db.exec('COMMIT')
  }

  private hasQuery?: StatementSync
  async has(cid: CID): Promise<boolean> {
    assert(!this.db.isTransaction, 'db is in transaction, likely loading')
    const hasQuery = (this.hasQuery ??= this.db.prepare(
      'SELECT cid FROM blockstore WHERE cid = ?',
    ))
    return hasQuery.get(cid.toString()) != null
  }

  private getBytesQuery?: StatementSync
  async getBytes(cid: CID): Promise<Uint8Array | null> {
    assert(!this.db.isTransaction, 'db is in transaction, likely loading')
    const getBytesQuery = (this.getBytesQuery ??= this.db.prepare(
      'SELECT bytes FROM blockstore WHERE cid = ?',
    ))
    const result = getBytesQuery.get(cid.toString())
    if (!result) return null
    return result.bytes as Uint8Array
  }

  private getBlocksQuery?: StatementSync
  async getBlocks(cids: CID[]): Promise<{ blocks: BlockMap; missing: CID[] }> {
    assert(!this.db.isTransaction, 'db is in transaction, likely loading')
    const missing: CID[] = []
    const blocks = new BlockMap()
    const getBlocksQuery = (this.getBlocksQuery ??= this.db.prepare(
      'SELECT cid, bytes FROM blockstore WHERE cid in (SELECT value FROM json_each(?))',
    ))
    for (const chunk of chunkArray(cids, 500)) {
      for (const { cid, bytes } of getBlocksQuery.all(JSON.stringify(chunk))) {
        blocks.set(CID.parse(cid as string), bytes as Uint8Array)
      }
      for (const cid of chunk) {
        if (!blocks.has(cid)) missing.push(cid)
      }
      await setImmediate()
    }
    return { blocks, missing }
  }
}
