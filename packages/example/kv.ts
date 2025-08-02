import { AckPolicy, jetstream, jetstreamManager } from '@nats-io/jetstream'
import { Kvm } from '@nats-io/kv'
import { connect } from '@nats-io/transport-node'
import { type CommitEvent } from '@skyware/jetstream'
import { recordIdToKey } from './util.ts'

async function main() {
  const connection = await connect({ servers: '0.0.0.0:4222' })
  const js = jetstream(connection)
  const jsm = await jetstreamManager(connection)
  const recordkv = await new Kvm(js).create('record', { history: 1 })
  await jsm.consumers.add('ingest', {
    durable_name: 'ingest-kv-consumer',
    ack_policy: AckPolicy.Explicit,
  })
  const consumer = await js.consumers.get('ingest', 'ingest-kv-consumer')
  const messages = await consumer.consume()
  for await (const item of messages) {
    const { did, commit } = item.json<CommitEvent<string>>()
    const { collection, rkey } = commit
    const partition = item.subject.split('.').pop()!
    if (commit.operation === 'create' || commit.operation === 'update') {
      await recordkv.put(
        recordIdToKey({ did, collection, rkey, partition }),
        JSON.stringify(commit.record),
      )
    } else if (commit.operation === 'delete') {
      await recordkv.delete(recordIdToKey({ did, collection, rkey, partition }))
    }
    item.ack()
  }
}

main()
