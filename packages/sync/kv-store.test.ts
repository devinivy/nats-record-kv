import assert from 'node:assert'
import { randomInt } from 'node:crypto'
import test from 'node:test'
import { DatabaseSync } from 'node:sqlite'
import { SqliteKvStore, type KvKey, type KvValue } from './kv-store.ts'

test('SqliteKvStore', async () => {
  function randomPop<T>(arr: T[]): T | undefined {
    if (!arr.length) return
    return arr.splice(randomInt(arr.length), 1).at(0)
  }
  function numericKey(int: number, len = 5): string {
    return String(int).padStart(len, '0')
  }
  function* cell(a: number, b: number) {
    for (let i = 0; i < a; ++i) {
      for (let j = 0; j < b; ++j) {
        yield [i, j, i * a + j] satisfies [number, number, number]
      }
    }
  }

  await test('put and get', async () => {
    const db = new DatabaseSync(':memory:')
    const kv = new SqliteKvStore(db)
    await kv.put(['a', 'b'], 'value1')
    const result = await kv.get(['a', 'b'])
    assert.strictEqual(result, 'value1')
  })

  await test('missing get', async () => {
    const db = new DatabaseSync(':memory:')
    const kv = new SqliteKvStore(db)
    const result = await kv.get(['a', 'b'])
    assert.strictEqual(result, null)
  })

  await test('scan nested', { skip: false }, async () => {
    const db = new DatabaseSync(':memory:')
    const kv = new SqliteKvStore(db)
    const inserts: [KvKey, KvValue][] = []
    for (const [i, j, idx] of cell(50, 50)) {
      inserts.push([['ns', numericKey(i), numericKey(j)], `${idx}`])
    }
    // Add all items into the kv in random order
    let insert: [KvKey, KvValue] | undefined
    while ((insert = randomPop(inserts))) {
      await kv.put(...insert)
    }
    // Scan full namespace
    let results: [KvKey, KvValue][] = []
    for await (const result of kv.scan(['ns'])) {
      results.push(result)
    }
    for (const [i, j, idx] of cell(50, 50)) {
      assert.deepStrictEqual(results[idx], [
        ['ns', numericKey(i), numericKey(j)],
        `${idx}`,
      ])
    }
    // Scan sub-namespace
    results = []
    for await (const result of kv.scan(['ns', numericKey(20)])) {
      results.push(result)
    }
    assert.strictEqual(results.length, 50)
    for (const [_, j] of cell(1, 50)) {
      assert.deepStrictEqual(results[j], [
        ['ns', numericKey(20), numericKey(j)],
        `${j + 20 * 50}`,
      ])
    }
  })

  await test('scan lexicographical', { skip: false }, async () => {
    const db = new DatabaseSync(':memory:')
    const kv = new SqliteKvStore(db)
    await kv.put(['ns', 'a0'], '2')
    await kv.put(['ns', 'a'], '1')
    // Scan full namespace
    const results: [KvKey, KvValue][] = []
    for await (const result of kv.scan(['ns'])) {
      results.push(result)
    }
    assert.deepStrictEqual(results, [
      [['ns', 'a'], '1'],
      [['ns', 'a0'], '2'],
    ])
  })
})
