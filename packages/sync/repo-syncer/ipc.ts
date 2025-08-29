import assert from 'node:assert'
import { fork, type ChildProcess } from 'node:child_process'
import { on } from 'node:events'
import { cpus } from 'node:os'
import process from 'node:process'
import { Readable, Writable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { fileURLToPath } from 'node:url'

const START = '__START__'
const END = '__END__'
const DONE = '__DONE__'
const ERROR = '__ERROR__'

export function createWorker<I, O>(
  mod: ImportMeta,
  run: (input: AsyncIterable<I>) => AsyncIterable<O>,
) {
  if (process.send) {
    // this is a forked child process
    ;(async () => {
      while (true) {
        await pipeline(
          ipcReadable(process),
          async function* (messages) {
            let started = false
            for await (const [code, msg] of messages) {
              if (code === START) {
                assert(!started, 'started')
                started = true
              } else if (code === END) {
                assert(started, 'not started')
                return
              } else {
                assert(started, 'not started')
                yield msg as I
              }
            }
          },
          async function* (input) {
            try {
              for await (const output of run(input)) {
                yield [null, output]
              }
              yield [DONE]
            } catch (err: unknown) {
              yield [
                ERROR,
                { message: err?.['message'], stack: err?.['stack'] },
              ]
            }
          },
          ipcWritable(process),
          { end: false },
        )
      }
    })()
    return function () {
      assert.fail('cannot invoke worker from within process')
    }
  }

  // this is used by the parent/caller
  return function spawnWorker() {
    const proc = fork(fileURLToPath(mod.url))
    async function* call(
      input: AsyncIterable<I>,
      autoExit = true,
    ): AsyncIterable<O> {
      // @TODO handle failure, signal completion
      const ac = new AbortController()
      proc.once('error', (err) => ac.abort(err))
      pipeline(
        input,
        async function* (input) {
          yield [START]
          for await (const item of input) {
            yield [null, item]
          }
          yield [END]
        },
        ipcWritable(proc),
        { end: false, signal: ac.signal },
      ).catch((err) => {
        assert(ac.signal.aborted, err)
      })
      try {
        for await (const [code, msg] of ipcReadable(proc)) {
          if (code === DONE) {
            return
          } else if (code === ERROR) {
            // @TODO tidy error
            const error = new Error(msg.message)
            if (msg.stack) error.stack = msg.stack
            throw error
          } else {
            yield msg as O
          }
        }
      } finally {
        ac.abort()
        if (autoExit) proc.kill()
      }
    }
    return { proc, call }
  }
}

type WorkerHandle<I, O> = {
  proc: ChildProcess
  call: (input: AsyncIterable<I>, autoExit: boolean) => AsyncIterable<O>
}

// @TODO replace crashed worker?
export function workerPool<I, O>(
  spawnWorker: () => WorkerHandle<I, O>,
  size = cpus().length,
) {
  const workers: { handle: WorkerHandle<I, O>; busy: boolean }[] = []
  const queue: {
    input: AsyncIterable<I>
    resolve: (out: AsyncIterable<O>) => void
  }[] = []

  for (let i = 0; i < size; i++) {
    workers.push({ handle: spawnWorker(), busy: false })
  }

  function dispatch() {
    const worker = workers.find((w) => !w.busy)
    if (!worker) return
    const job = queue.shift()
    if (!job) return

    const { input, resolve } = job

    worker.busy = true
    resolve(
      (async function* output() {
        try {
          yield* worker.handle.call(input, false)
        } finally {
          // when the generator finishes (consumer fully iterates or stops)
          worker.busy = false
          dispatch() // trigger next job in queue
        }
      })(),
    )
  }

  function run(input: AsyncIterable<I>): AsyncIterable<O> {
    return {
      async *[Symbol.asyncIterator]() {
        const output = await new Promise<AsyncIterable<O>>((resolve) => {
          queue.push({ input, resolve })
          dispatch()
        })
        yield* output
      },
    }
  }

  function destroy() {
    for (const w of workers) {
      w.handle.proc.kill()
    }
  }

  return { run, destroy }
}

function ipcWritable(proc: NodeJS.Process | ChildProcess) {
  assert(proc.send, 'process must have send() ipc channel')
  return new Writable({
    objectMode: true,
    write(chunk, _, cb) {
      proc.send!(chunk, cb)
    },
  })
}

function ipcReadable(proc: NodeJS.Process | ChildProcess) {
  const messages = async function* () {
    for await (const [msg] of on(proc, 'message')) {
      yield msg
    }
  }
  return Readable.from(messages(), { objectMode: true })
}
