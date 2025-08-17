import { stringifyLex } from '@atproto/lexicon'
import { Subscription } from '@atproto/xrpc-server'
import {
  DiscardPolicy,
  jetstream,
  JetStreamApiError,
  jetstreamManager,
  RetentionPolicy,
  type JetStreamClient,
} from '@nats-io/jetstream'
import { Kvm, type KV } from '@nats-io/kv'
import { connect } from '@nats-io/transport-node'
import { default as murmur } from 'murmurhash'
import { ConsecutiveList } from './consecutive-list.ts'
import type { SubscribeReposMessage } from './types.ts'

// This process yeets the firehose into a durable buffer which
// is used to apply backpressure on firehose ingestion. It is also
// partitioned.

type IngestOptions = {
  natsHost?: string
  firehoseHost?: string
}

export async function createFirehoseIngest(opts: IngestOptions = {}) {
  const connection = await connect({ servers: opts.natsHost })
  const js = jetstream(connection)
  const jsm = await jetstreamManager(connection)
  const cursorkv = await new Kvm(js).create('cursor', { history: 1 })
  await jsm.streams.add({
    name: 'firehose',
    subjects: ['firehose.*'], // firehose.{partition}
    discard: DiscardPolicy.New, // reject new messages as backpressure once full
    retention: RetentionPolicy.Workqueue, // retain unacked messages
    max_msgs: 5000,
    max_msg_size: 1024 * 1024,
    allow_direct: true,
  })
  const cursor = new Cursor(cursorkv, 'firehose')
  await cursor.init()
  while (true) {
    const ac = new AbortController()
    const lock = new Lock()
    const consecutive = new ConsecutiveList<number>()
    const firehose = createSubscription({
      host: opts.firehoseHost,
      signal: ac.signal,
      getCursor: () => cursor.current,
    })
    try {
      for await (const msg of firehose) {
        if (lock.size >= 1000) ac.abort()
        if (ac.signal.aborted) break
        if (
          msg.$type === 'com.atproto.sync.subscribeRepos#commit' ||
          msg.$type === 'com.atproto.sync.subscribeRepos#account' ||
          msg.$type === 'com.atproto.sync.subscribeRepos#identity' ||
          msg.$type === 'com.atproto.sync.subscribeRepos#sync'
        ) {
          const did = 'repo' in msg ? msg.repo : msg.did
          const item = consecutive.push(msg.seq)
          const partition = getPartition(did)
          const payload = stringifyLex(msg)
          lock
            .run(did, async () => {
              if (ac.signal.aborted) return
              await publish(js, `firehose.${partition}`, payload)
              const latest = item.complete().at(-1)
              if (latest != null) {
                await cursor.set(latest)
              }
            })
            .catch(() => ac.abort())
        } else if (msg.$type === 'com.atproto.sync.subscribeRepos#info') {
          console.warn('firehose info', msg.name, msg.message) // @TODO logger
        } else {
          console.warn('unhandled firehose event', msg['seq'], msg['$type']) // @TODO logger
        }
      }
    } catch {
      // no-op
    } finally {
      // Aborted, e.g. due to backpressure. Sync and then wait.
      console.warn('applying backpressure, restarting')
      await cursor.sync()
      await wait(1000)
    }
  }
}

async function publish(js: JetStreamClient, subject: string, payload: string) {
  try {
    await js.publish(subject, payload)
  } catch (err) {
    console.warn('publish failed', err['message'])
    if (isBackpressureErr(err)) throw err
    await wait(200)
    await publish(js, subject, payload)
  }
}

class Cursor {
  private kv: KV
  private cursor: number | undefined
  private persisted: number | undefined
  private lock: Lock
  readonly name: string

  constructor(kv: KV, name: string) {
    this.kv = kv
    this.name = name
    this.lock = new Lock()
  }

  get current() {
    return this.cursor
  }

  async init() {
    this.cursor = this.persisted = await this.get()
  }

  async get() {
    const cursorEntry = await this.kv.get(this.name)
    return cursorEntry ? parseInt(cursorEntry.string(), 10) : undefined
  }

  async set(latest: number) {
    if (this.cursor != null && latest <= this.cursor) return
    this.cursor = latest
    await this.sync()
  }

  async sync() {
    await this.lock.run('sync', async () => {
      const persist = this.cursor
      if (!persist || persist === this.persisted) return
      await this.kv.put(this.name, persist.toString())
      this.persisted = persist
    })
  }
}

function createSubscription(opts: {
  host?: string
  getCursor?: () => number | undefined
  signal?: AbortSignal
}) {
  return new Subscription<SubscribeReposMessage>({
    service: opts.host ?? 'wss://bsky.network',
    method: 'com.atproto.sync.subscribeRepos',
    signal: opts.signal,
    getParams: () => {
      const cursor = opts.getCursor?.()
      return { cursor }
    },
    validate: (value: unknown): SubscribeReposMessage => {
      // @TODO validation?
      return value as SubscribeReposMessage
    },
  })
}

class Lock {
  running = new Map<string, Promise<unknown>>()
  size = 0
  async run<T>(name: string, fn: () => Promise<T>) {
    this.size++
    let running: Promise<unknown> | undefined
    while ((running = this.running.get(name))) {
      await running.catch(() => null)
    }
    const promise = fn().finally(() => {
      this.size--
      this.running.delete(name)
    })
    this.running.set(name, promise)
    return promise
  }
}

// @NOTE only 16 partitions here, but could do more.
function getPartition(str: string) {
  return murmur.v3(str).toString(16).padStart(2, '0').slice(0, 1)
}

export async function wait(ms: number) {
  return new Promise((res) => setTimeout(res, ms))
}

const STORE_MESSAGE_FAIL_CODE = 10077
function isBackpressureErr(err: unknown) {
  return (
    err instanceof JetStreamApiError && err.code === STORE_MESSAGE_FAIL_CODE
  )
}

// start if run directly, e.g. node ingest.ts
if (import.meta.url === `file://${process.argv[1]}`) {
  createFirehoseIngest()
}
