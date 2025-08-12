import {
  DiscardPolicy,
  jetstream,
  JetStreamApiError,
  jetstreamManager,
  RetentionPolicy,
} from '@nats-io/jetstream'
import { Kvm } from '@nats-io/kv'
import { connect } from '@nats-io/transport-node'
import { Jetstream as Firehose } from '@skyware/jetstream'
import { getPartition, STORE_MESSAGE_FAIL_CODE, wait } from './util.ts'

type IngestOptions = {
  host?: string
}

export async function createFirehoseIngest(opts: IngestOptions = {}) {
  const connection = await connect({ servers: opts.host })
  const js = jetstream(connection)
  const jsm = await jetstreamManager(connection)
  const cursorkv = await new Kvm(js).create('cursor', { history: 1 })
  await jsm.streams.add({
    name: 'ingest',
    subjects: ['ingest.*'], // ingest.{partition}
    discard: DiscardPolicy.New, // reject new messages as backpressure once full
    retention: RetentionPolicy.Workqueue, // retain unacked messages
    max_msgs: 5000,
    max_msg_size: 1024 * 1024,
  })
  const cursorEntry = await cursorkv.get('ingest')
  const cursor = cursorEntry ? parseInt(cursorEntry.string(), 10) : undefined
  const firehose = new Firehose({ cursor })
  let seq = cursorEntry?.revision
  let running = false
  firehose
    .on('commit', async (event) => {
      if (!running) return
      const partition = getPartition(event.did)
      await js
        .publish(`ingest.${partition}`, JSON.stringify(event))
        .catch(async (err) => {
          if (
            err instanceof JetStreamApiError &&
            err.code === STORE_MESSAGE_FAIL_CODE // occurs when stream is full, backpressure
          ) {
            if (!running) return
            console.warn(
              `restarting firehose ingest due to backpressure (seq ${seq ?? 'none'})`,
            )
            running = false
            firehose.close()
            await wait(1000)
            running = true
            firehose.start()
          } else {
            throw err
          }
        })
      // @NOTE cursor management would need a little more love to ensure there are no gaps during restart/crashes.
      seq = await cursorkv
        .put('ingest', event.time_us.toString(), { previousSeq: seq })
        .catch(() => seq)
    })
    .start()
  running = true
}

// start if run directly, e.g. node ingest.ts
if (import.meta.url === `file://${process.argv[1]}`) {
  createFirehoseIngest()
}
