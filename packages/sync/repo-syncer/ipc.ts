import { fork, type ChildProcess } from 'node:child_process'
import { cpus } from 'node:os'
import { exit, send, stdin, stdout } from 'node:process'
import readline from 'node:readline'
import { fileURLToPath } from 'node:url'

const START = '__START__'
const END = '__END__'
const DONE = '__DONE__'
const ERROR = '__ERROR__'

type ControlCode = typeof START | typeof END | typeof DONE | typeof ERROR

function isControlCode(line: string, code: ControlCode) {
  return line.startsWith(code)
}

export function createWorker<I = unknown, O = unknown>(
  mod: ImportMeta,
  run: (input: AsyncIterable<I>) => AsyncIterable<O>,
) {
  if (!send) {
    // this is a forked child process
    ;(async () => {
      const rl = readline.createInterface({
        input: stdin,
        crlfDelay: Infinity,
      })

      let buffer: I[] = []

      async function* inputStream(items: I[]) {
        for (const i of items) yield i
      }

      for await (const line of rl) {
        if (isControlCode(line, START)) {
          buffer = []
        } else if (isControlCode(line, END)) {
          try {
            const iter = inputStream(buffer)
            for await (const val of run(iter)) {
              stdout.write(JSON.stringify(val) + '\n')
            }
            stdout.write(`${DONE}\n`)
          } catch (err: unknown) {
            stdout.write(
              ERROR +
                JSON.stringify({
                  message: err?.['message'],
                  stack: err?.['stack'],
                }) +
                '\n',
            )
          }
        } else {
          buffer.push(JSON.parse(line) as I)
        }
      }
    })().catch((err) => {
      console.error(err)
      exit(1)
    })
    return
  }

  // this is used by the parent/caller
  return function spawnWorker() {
    const proc = fork(fileURLToPath(mod.url), [], {
      stdio: ['pipe', 'pipe', 'inherit'],
    })

    async function* call(
      input: AsyncIterable<I>,
      autoExit = true,
    ): AsyncIterable<O> {
      proc.stdin!.write(`${START}\n`)
      for await (const item of input) {
        proc.stdin!.write(JSON.stringify(item) + '\n')
      }
      proc.stdin!.write(`${END}\n`)

      const rl = readline.createInterface({
        input: proc.stdout!,
        crlfDelay: Infinity,
      })

      let error: Error | null = null

      try {
        for await (const line of rl) {
          if (isControlCode(line, DONE)) break
          if (isControlCode(line, ERROR)) {
            const payload = JSON.parse(line.slice(ERROR.length))
            error = new Error(payload.message)
            if (payload.stack) error.stack = payload.stack
            break
          }
          yield JSON.parse(line) as O
        }
      } finally {
        rl.close()
        if (autoExit) {
          proc.kill()
        }
      }

      if (error) {
        throw error
      }
    }

    return { proc, call }
  }
}

type WorkerHandle<I, O> = {
  proc: ChildProcess
  call: (input: AsyncIterable<I>, autoExit: boolean) => AsyncIterable<O>
}

export function WorkerPool<I, O>(
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
          for await (const val of worker.handle.call(input, false)) {
            yield val // yield results to consumer
          }
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
      [Symbol.asyncIterator]() {
        return new Promise<AsyncIterator<O>>((resolve) => {
          queue.push({
            input,
            resolve: (out) => resolve(out[Symbol.asyncIterator]()),
          })
          dispatch()
        }) as unknown as AsyncIterator<O>
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
