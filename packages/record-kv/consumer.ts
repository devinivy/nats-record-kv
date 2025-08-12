import {
  AckPolicy,
  DeliverPolicy,
  jetstream,
  jetstreamManager,
  type JsMsg,
} from '@nats-io/jetstream'
import { connect } from '@nats-io/transport-node'
import {
  getKvUpdate,
  keyToRecordId,
  selectCollection,
  type RecordId,
} from './util.ts'

type ConsumerOptions = {
  name: string
  collection: string | string[]
  host?: string
}

export async function* consumer(
  opts: ConsumerOptions,
): AsyncGenerator<[RecordId, unknown, JsMsg]> {
  const { host, collection, name } = opts
  const connection = await connect({ servers: host })
  const js = jetstream(connection)
  const jsm = await jetstreamManager(connection)
  const collections = typeof collection === 'string' ? [collection] : collection
  await jsm.consumers.add('KV_record', {
    durable_name: name,
    filter_subjects: collections.map(selectCollection),
    ack_policy: AckPolicy.Explicit,
    deliver_policy: DeliverPolicy.All,
  })
  const consumer = await js.consumers.get('KV_record', 'like-indexer')
  const messages = await consumer.consume()
  for await (const msg of messages) {
    const { key, value: record } = getKvUpdate(msg)
    const id = keyToRecordId(key)
    yield [id, record, msg]
  }
  await messages.close()
}
