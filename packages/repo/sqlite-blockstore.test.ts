import { Readable } from 'node:stream'
import test from 'node:test'
import { def, MST, readCarStream } from '@atproto/repo'
import { DatabaseSync } from 'node:sqlite'
import { SqliteBlockstore } from './sqlite-blockstore.ts'

test('SqliteBlockstore', async () => {
  await test('loads', async () => {
    const db = new DatabaseSync('pfrazee.com.sqlite', { allowExtension: true })
    db.exec('PRAGMA journal_mode=WAL')
    const blockstore = new SqliteBlockstore(db)
    const url = new URL(
      '/xrpc/com.atproto.sync.getRepo',
      'https://morel.us-east.host.bsky.network',
    )
    url.searchParams.set('did', 'did:plc:ragtjsm2j2vknwkz3zp4oxrd') // paul
    console.time('fetch')
    const result = await fetch(url)
    if (!result.ok) {
      throw new Error('request failed:' + (await result.text()))
    }
    const repoStream = Readable.from(result.body ?? '')
    console.timeEnd('fetch')
    console.time('load')

    const { roots, blocks } = await readCarStream(repoStream)
    await blockstore.load(blocks)
    console.timeEnd('load')
    console.time('walk')
    const commit = await blockstore.readObj(roots[0], def.versionedCommit)
    const mst = MST.load(blockstore, commit.data)
    let count = 0
    for await (const _ of mst.walkLeavesFrom('')) {
      count++
    }
    console.timeEnd('walk')
    console.log({ count })
  })
})
