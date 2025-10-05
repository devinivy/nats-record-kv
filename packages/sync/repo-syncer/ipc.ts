import assert from 'node:assert'
import { fork, type ChildProcess } from 'node:child_process'
import EventEmitter, { on, once } from 'node:events'
import { cpus } from 'node:os'
import process from 'node:process'
import { PassThrough, Readable, Writable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { fileURLToPath } from 'node:url'

const START = 1
const MSG = 2
const END = 3
const DONE = 4
const ERROR = -1

export function createWorker<I, O>(
  mod: ImportMeta,
  run: (input: AsyncIterable<I>) => AsyncIterable<O>,
): Worker<I, O> {
  if (process.send) {
    // this is a forked child process
    ;(async () => {
      const handle = new IpcChildWorkerHandle(process)
      const runner = new Runner(handle, run)
      try {
        await runner.complete()
      } finally {
        handle.destroy()
      }
    })()
    return Object.assign(
      async function () {
        assert.fail('cannot invoke a worker from within a worker')
      },
      {
        spawn() {
          assert.fail('cannot spawn a worker from within a worker')
        },
      },
    ) as unknown as Worker<I, O>
  }
  // this is used by the parent/caller
  const spawn = () => new IpcWorkerHandle(spawnProc(mod))
  return Object.assign(
    async function* (input: AsyncIterable<I>) {
      const handle = spawn()
      const executor = new Executor(handle)
      try {
        yield* executor.exec<I, O>(input)
      } finally {
        handle.destroy()
      }
    },
    { spawn },
  )
}

type Worker<I, O> = {
  (input: AsyncIterable<I>): AsyncIterable<O>
  spawn(): WorkerHandle
}

type RunState<I, O> = {
  id: ExecId
  ac: AbortController
  execin: PassThroughOf<InputProtocol<I>>
  execout: PassThroughOf<OutputProtocol<O>>
  execoutIter: AsyncIterator<OutputProtocol<O>>
}

class Runner<I, O> {
  private handle: WorkerHandle<'child'>
  private run: (input: AsyncIterable<I>) => AsyncIterable<O>
  private ee: EventEmitter<{ run: [ExecId] }>
  private state: Map<number, RunState<I, O>>
  private running: Promise<void>
  constructor(
    handle: WorkerHandle<'child'>,
    run: (input: AsyncIterable<I>) => AsyncIterable<O>,
  ) {
    this.handle = handle
    this.run = run
    this.ee = new EventEmitter()
    this.state = new Map()
    this.running = this.initialize()
  }
  async complete() {
    await this.running
  }
  private async initialize() {
    await Promise.allSettled([
      pipeline(this.handle.procin, this.demux.bind(this)),
      pipeline(this.mux(), this.handle.procout),
    ])
  }
  private async *mux() {
    // @TODO need to follow abort, or is that handled sufficiently elsewhere?
    const promises = new Map<number, Promise<PendingExec>>()
    this.ee.on('run', (id) => {
      const item = this.state.get(id)
      if (item) promises.set(id, next(id, item.execoutIter))
    })
    while (!this.handle.signal.aborted) {
      while (promises.size) {
        const [id, result] = await Promise.race(promises.values())
        promises.delete(id)
        if (result.done) continue
        yield result.value
        const item = this.state.get(id)
        if (item) promises.set(id, next(id, item.execoutIter))
      }
      await once(this.ee, 'run', { signal: this.handle.signal }).catch(
        () => null, // @TODO / ignore cancel
      )
    }
    promises.clear()
  }
  private async demux(procin: AsyncIterable<InputProtocol<unknown>>) {
    const pending = new Map<number, Promise<void>>()
    for await (const item of procin) {
      if (item[0] === START) this.beginRun(item[1])
      const run = this.state.get(item[1])
      if (!run) continue
      const { id, execin } = run
      if (pending.has(id)) await pending.get(id)
      if (!execin.writable) continue
      const ok = execin.write(item)
      if (!ok) {
        pending.set(
          id,
          drainOrClose(execin).finally(() => pending.delete(id)),
        )
      }
    }
  }
  private beginRun(id: ExecId) {
    const { ac, execin, execout } = this.trackRun(id)
    this.handle.signal.addEventListener('abort', (err) => ac.abort(err), {
      signal: ac.signal,
    })
    // @TODO lifecycle management. early abort tearsdown execin?
    pipeline(
      execin,
      inputProtocolOffload,
      this.run.bind(this),
      outputProtocolOnload.bind({ id }),
      execout,
      { signal: ac.signal },
    )
      .catch((err) => {
        if (ac.signal.reason === err?.['cause']) return
        throw err
      })
      .finally(() => this.untrackRun(id))
  }
  private trackRun(id: ExecId): RunState<I, O> {
    const ac = new AbortController()
    assert(!this.state.has(id), 'protocol error: run already started')
    const execin: PassThroughOf<InputProtocol<I>> = new PassThrough({
      objectMode: true,
    })
    const execout: PassThroughOf<OutputProtocol<O>> = new PassThrough({
      objectMode: true,
    })
    const execoutIter = execout[Symbol.asyncIterator]()
    const state = { id, ac, execin, execout, execoutIter }
    this.state.set(id, state)
    this.ee.emit('run', id)
    return state
  }
  private untrackRun(id: ExecId) {
    this.state.delete(id)
  }
}

// @TODO replace crashed worker?
export function workerPool<I, O>(
  worker: Worker<I, O>,
  size = cpus().length,
  alwaysSchedule = false,
) {
  const workers: { handle: WorkerHandle; executor: Executor; busy: number }[] =
    []
  const queue: {
    input: AsyncIterable<I>
    resolve: (out: AsyncIterable<O>) => void
  }[] = []

  for (let i = 0; i < size; i++) {
    const handle = worker.spawn()
    const executor = new Executor(handle)
    workers.push({ handle, executor, busy: 0 })
  }

  function dispatch() {
    const worker = alwaysSchedule
      ? workers.reduce((min, w) => (w.busy < min.busy ? w : min), workers[0])
      : workers.find((w) => w.busy === 0)
    if (!worker) return
    const job = queue.shift()
    if (!job) return

    const { input, resolve } = job

    worker.busy++
    resolve(
      (async function* output() {
        try {
          yield* worker.executor.exec<I, O>(input)
        } finally {
          // when the generator finishes (consumer fully iterates or stops)
          worker.busy--
          dispatch() // trigger next job in queue
        }
      })(),
    )
  }

  return Object.assign(
    function (input: AsyncIterable<I>): AsyncIterable<O> {
      return {
        async *[Symbol.asyncIterator]() {
          const output = await new Promise<AsyncIterable<O>>((resolve) => {
            queue.push({ input, resolve })
            dispatch()
          })
          yield* output
        },
      }
    },
    {
      destroy() {
        for (const w of workers) {
          w.handle.destroy()
        }
      },
      [Symbol.dispose]() {
        this.destroy()
      },
    },
  )
}

/*
 *
 * call1: input -> protoin \ - - - - -- - - - - - / protoout -> output
 *                          -> procin | procout ->
 * call2: input -> protoin / - - - - - -- - - - - \ protoout -> output
 *
 */

type ExecId = number

type InputProtocol<T> =
  | [typeof START, ExecId]
  | [typeof MSG, ExecId, T]
  | [typeof END, ExecId]

type OutputProtocol<T> =
  | [typeof MSG, ExecId, T]
  | [typeof DONE, ExecId]
  | [typeof ERROR, ExecId, { name?: string; message?: string; stack?: string }]

type ExecState<I, O> = {
  id: ExecId
  ac: AbortController
  execin: PassThroughOf<InputProtocol<I>>
  execinIter: AsyncIterator<InputProtocol<I>>
  execout: PassThroughOf<OutputProtocol<O>>
}

class Executor {
  private handle: WorkerHandle
  private execId: ExecId
  private ee: EventEmitter<{ exec: [ExecId] }>
  private state: Map<number, ExecState<unknown, unknown>>
  constructor(handle: WorkerHandle) {
    this.handle = handle
    this.execId = 0
    this.ee = new EventEmitter()
    this.state = new Map()
    this.initialize()
  }
  private async initialize() {
    await Promise.allSettled([
      pipeline(this.mux(), this.handle.procin),
      pipeline(this.handle.procout, this.demux.bind(this)),
    ])
  }
  private async *mux() {
    // @TODO need to follow abort, or is that handled sufficiently in exec()?
    const promises = new Map<number, Promise<PendingExec>>()
    this.ee.on('exec', (id) => {
      const item = this.state.get(id)
      if (item) promises.set(id, next(id, item.execinIter))
    })
    while (!this.handle.signal.aborted) {
      while (promises.size) {
        const [id, result] = await Promise.race(promises.values())
        promises.delete(id)
        if (result.done) continue
        yield result.value
        const item = this.state.get(id)
        if (item) promises.set(id, next(id, item.execinIter))
      }
      await once(this.ee, 'exec', { signal: this.handle.signal }).catch(
        () => null, // @TODO / ignore cancel
      )
    }
    promises.clear()
  }
  private async demux(procout: AsyncIterable<OutputProtocol<unknown>>) {
    const pending = new Map<number, Promise<void>>()
    for await (const result of procout) {
      const exec = this.state.get(result[1])
      if (!exec) continue
      const { id, execout } = exec
      if (pending.has(id)) await pending.get(id)
      if (!execout.writable) continue
      const ok = execout.write(result)
      if (!ok) {
        pending.set(
          id,
          drainOrClose(execout).finally(() => pending.delete(id)),
        )
      }
    }
  }
  async *exec<I, O>(input: AsyncIterable<I>): AsyncIterable<O> {
    const { id, ac, execin, execout } = this.trackExec<I, O>()
    this.handle.signal.addEventListener('abort', (err) => ac.abort(err), {
      signal: ac.signal,
    })
    pipeline(Readable.from(input), inputProtocolOnload.bind({ id }), execin, {
      signal: ac.signal,
    }).catch((err) => {
      if (ac.signal.reason === err?.['cause']) return
      throw err
    })
    try {
      for await (const result of outputProtocolOffload(execout)) {
        if (ac.signal.aborted) return
        yield result
      }
    } finally {
      ac.abort()
      this.untrackExec(id)
    }
  }
  private trackExec<I, O>(): ExecState<I, O> {
    const id = this.execId++
    const ac = new AbortController()
    const execin: PassThroughOf<InputProtocol<I>> = new PassThrough({
      objectMode: true,
    })
    const execinIter = execin[Symbol.asyncIterator]()
    const execout: PassThroughOf<OutputProtocol<O>> = new PassThrough({
      objectMode: true,
    })
    const state = { id, ac, execin, execinIter, execout }
    this.state.set(id, state)
    this.ee.emit('exec', id)
    return state
  }
  private untrackExec(id: number) {
    this.state.delete(id)
  }
}

type PendingExec = [ExecId, IteratorResult<unknown>]

async function next<T>(
  id: ExecId,
  iterator: AsyncIterator<T>,
): Promise<PendingExec> {
  try {
    return [id, await iterator.next()]
  } catch {
    return [id, { value: undefined, done: true }]
  }
}

async function* inputProtocolOnload<I>(
  this: { id: ExecId },
  messages: AsyncIterable<I>,
): AsyncGenerator<InputProtocol<I>> {
  yield [START, this.id]
  for await (const msg of messages) {
    yield [MSG, this.id, msg]
  }
  yield [END, this.id]
}

async function* outputProtocolOffload<O>(
  messages: AsyncIterable<OutputProtocol<O>>,
): AsyncGenerator<O> {
  for await (const result of messages) {
    console.log([result[0], result[1]])
    if (result[0] === MSG) yield result[2]
    if (result[0] === DONE) return
    if (result[0] === ERROR) {
      const { name, message, stack } = result[2]
      throw Object.assign(new Error(message), { name, stack })
    }
  }
}

async function* inputProtocolOffload<I>(
  messages: AsyncIterable<InputProtocol<I>>,
): AsyncGenerator<I> {
  for await (const result of messages) {
    if (result[0] === START) continue
    if (result[0] === MSG) yield result[2]
    if (result[0] === END) return
  }
}

async function* outputProtocolOnload<O>(
  this: { id: ExecId },
  messages: AsyncIterable<O>,
): AsyncGenerator<OutputProtocol<O>> {
  try {
    for await (const result of messages) {
      yield [MSG, this.id, result]
    }
    yield [DONE, this.id]
  } catch (err) {
    yield [
      ERROR,
      this.id,
      {
        // @TODO type
        name: err.name,
        message: err.message,
        stack: err.stack,
      },
    ]
  }
}

function spawnProc(mod: ImportMeta) {
  return fork(fileURLToPath(mod.url))
}

type PassThroughOf<T> = AsyncIterable<T> &
  Omit<PassThrough, typeof Symbol.asyncIterator>

interface WorkerHandle<Side extends 'child' | 'parent' = 'parent'> {
  procin: Side extends 'parent' ? Writable : Readable // InputProtocol<I>
  procout: Side extends 'parent' ? Readable : Writable // OutputProtocol<O>
  signal: AbortSignal
  destroy(): void
}

class IpcWorkerHandle implements WorkerHandle<'parent'> {
  private proc: ChildProcess
  procin: Writable
  procout: Readable
  private ac: AbortController
  signal: AbortSignal
  constructor(proc: ChildProcess) {
    this.proc = proc
    this.procin = ipcWritable(proc)
    this.procout = ipcReadable(proc)
    this.ac = new AbortController()
    this.signal = this.ac.signal
  }
  destroy() {
    this.ac.abort()
    this.proc.kill()
  }
}

class IpcChildWorkerHandle implements WorkerHandle<'child'> {
  private proc: NodeJS.Process
  procin: Readable
  procout: Writable
  private ac: AbortController
  signal: AbortSignal
  constructor(proc: NodeJS.Process) {
    this.proc = proc
    this.procin = ipcReadable(proc)
    this.procout = ipcWritable(proc)
    this.ac = new AbortController()
    this.signal = this.ac.signal
  }
  destroy() {
    this.ac.abort()
  }
}

function ipcWritable(proc: NodeJS.Process | ChildProcess) {
  assert(proc.send, 'process must have send() ipc channel')
  return new Writable({
    objectMode: true,
    write(chunk, _, cb) {
      // @TODO console.log('write', chunk, proc.connected)
      proc.send!(chunk, cb)
    },
  })
}

function ipcReadable(proc: NodeJS.Process | ChildProcess) {
  const messages = async function* () {
    for await (const [msg] of on(proc, 'message')) {
      // @TODO console.log('read', msg)
      yield msg
    }
  }
  return Readable.from(messages(), { objectMode: true })
}

async function drainOrClose(stream: Writable): Promise<void> {
  const ac = new AbortController()
  try {
    await Promise.race([
      once(stream, 'drain', { signal: ac.signal }),
      once(stream, 'close', { signal: ac.signal }),
    ])
  } catch {
    // no-op
  } finally {
    ac.abort()
  }
}
