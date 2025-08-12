import { fork } from 'node:child_process'
import { once } from 'node:events'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { wait } from './util.ts'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

async function main() {
  console.log('starting record kv builder')
  const ac = new AbortController()
  process.once('SIGINT', () => ac.abort())
  // firehose ingest into durable buffer, max size applies backpressure
  console.log('starting subprocess: firehose ingester')
  const ingest = fork(relative('ingest.ts'), { signal: ac.signal })
  ingest.once('exit', () => ac.abort())
  ingest.once('error', () => ac.abort())
  // leave a moment to setup stream and consumers
  await once(ingest, 'spawn')
  await wait(1000)
  // process firehose buffer into the record kv store
  console.log('starting subprocess: kv builder')
  const kv = fork(relative('kv.ts'), { signal: ac.signal })
  kv.once('exit', () => ac.abort())
  kv.once('error', () => ac.abort())
}

function relative(filename: string) {
  return path.join(__dirname, filename)
}

main()
