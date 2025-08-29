import assert from 'node:assert'
import { fork, type ChildProcess } from 'node:child_process'
import { cpus } from 'node:os'
import process, { stdin, stdout } from 'node:process'
import readline from 'node:readline'
import { pipeline } from 'node:stream/promises'
import { fileURLToPath } from 'node:url'

const START = '__START__'
const END = '__END__'
const DONE = '__DONE__'
const ERROR = '__ERROR__'

type ControlCode = typeof START | typeof END | typeof DONE | typeof ERROR

function isControlCode(line: string, code: ControlCode) {
  return line.startsWith(code)
}

const body = JSON.stringify

async function* linesToChunks(lines: AsyncIterable<string>) {
  for await (const line of lines) {
    yield Buffer.from(line + '\n')
  }
}

export function createWorker<I, O>(
  mod: ImportMeta,
  run: (input: AsyncIterable<I>) => AsyncIterable<O>,
) {
  if (process.send) {
    // this is a forked child process
    ;(async () => {
      while (true) {
        const inputLines = readline.createInterface({
          input: stdin,
          crlfDelay: Infinity,
        })
        await pipeline(
          inputLines,
          async function* (lines) {
            let started = false
            for await (const line of lines) {
              if (isControlCode(line, START)) {
                assert(!started, 'started')
                started = true
              } else if (isControlCode(line, END)) {
                assert(started, 'not started')
                return
              } else {
                assert(started, 'not started')
                yield JSON.parse(line) as I
              }
            }
          },
          async function* (input) {
            try {
              for await (const output of run(input)) {
                yield body(output)
              }
              yield DONE
            } catch (err: unknown) {
              yield ERROR +
                body({ message: err?.['message'], stack: err?.['stack'] })
            }
          },
          linesToChunks,
          stdout,
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
    const proc = fork(fileURLToPath(mod.url), [], {
      stdio: ['pipe', 'pipe', 'inherit', 'ipc'],
    })

    async function* call(
      input: AsyncIterable<I>,
      autoExit = true,
    ): AsyncIterable<O> {
      const outputLines = readline.createInterface({
        input: proc.stdout!,
        crlfDelay: Infinity,
      })
      // @TODO handle failure, signal completion
      const ac = new AbortController()
      proc.once('error', (err) => ac.abort(err))
      pipeline(
        input,
        async function* (input) {
          yield START
          for await (const item of input) {
            yield body(item)
          }
          yield END
        },
        linesToChunks,
        proc.stdin!,
        { end: false, signal: ac.signal },
      ).catch((err) => {
        assert(ac.signal.aborted, err)
      })
      try {
        for await (const line of outputLines) {
          if (isControlCode(line, DONE)) {
            return
          } else if (isControlCode(line, ERROR)) {
            // @TODO tidy error
            const payload = JSON.parse(line.slice(ERROR.length))
            const error = new Error(payload.message)
            if (payload.stack) error.stack = payload.stack
            throw error
          } else {
            yield JSON.parse(line) as O
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
