import { DidResolverCommon } from '@atproto-labs/did-resolver'
import { jsonToLex } from '@atproto/lexicon'
import { AckPolicy, jetstream, jetstreamManager } from '@nats-io/jetstream'
// import { Kvm } from '@nats-io/kv'
import { connect } from '@nats-io/transport-node'
import { DatabaseSync } from 'node:sqlite'
import { ActorStore } from '../actor-store.ts'
import { SqliteKvStore, type KvStore } from '../kv-store.ts'
import { RecordStore } from '../record-store.ts'
import type { SubscribeReposEvent } from '../types.ts'
import { account } from './account.ts'
import { commit } from './commit.ts'
import { identity } from './identity.ts'
import { sync } from './sync.ts'
import type { HexChar, SyncConsumerContext } from './util.ts'

// This process yeets the firehose into a durable buffer which
// is used to apply backpressure on firehose ingestion. It is also
// partitioned.

type SyncConsumerOptions = {
  natsHost?: string
  partitions?: HexChar[]
  kv?: KvStore
}

export async function createSyncConsumer(opts: SyncConsumerOptions = {}) {
  const connection = await connect({ servers: opts.natsHost })
  const js = jetstream(connection)
  const jsm = await jetstreamManager(connection)
  // const recordkv = await new Kvm(js).create('record', { history: 1 })
  const name = `sync-consumer-${opts.partitions?.join('-') ?? 'all'}`
  await jsm.consumers.add('firehose', {
    durable_name: name,
    filter_subjects: opts.partitions
      ? (opts.partitions ?? ['*']).map((p) => `firehose.${p}`)
      : ['firehose.*'],
    ack_policy: AckPolicy.Explicit,
  })
  const kv = opts.kv ?? new SqliteKvStore()
  const ctx: SyncConsumerContext = {
    actorStore: new ActorStore(kv),
    recordStore: new RecordStore(kv),
    didResolver: new DidResolverCommon(),
  }
  const consumer = await js.consumers.get('firehose', name)
  const messages = await consumer.consume()
  for await (const msg of messages) {
    const evt = jsonToLex(msg.json()) as SubscribeReposEvent
    if (evt.$type === 'com.atproto.sync.subscribeRepos#commit') {
      // verify, apply ops, may lead to sync
      await commit(evt, ctx)
    } else if (evt.$type === 'com.atproto.sync.subscribeRepos#sync') {
      // fetch repo, verify, diff
      await sync(evt, ctx)
    } else if (evt.$type === 'com.atproto.sync.subscribeRepos#account') {
      // update account hosting status
      await account(evt, ctx)
    } else if (evt.$type === 'com.atproto.sync.subscribeRepos#identity') {
      // refresh identity info
      await identity(evt, ctx)
    } else {
      console.warn('unhandled event', evt['seq'], evt.$type)
    }
    msg.ack()
  }
}

// start if run directly, e.g. node kv.ts
if (import.meta.url === `file://${process.argv[1]}`) {
  const db = new DatabaseSync('kv.sqlite')
  db.exec('PRAGMA journal_mode=WAL')
  createSyncConsumer({ kv: new SqliteKvStore(db) })
}
