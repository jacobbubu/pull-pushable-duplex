import * as pull from 'pull-stream'
import { SourceState } from './source-state'
import { SinkState } from './sink-state'
import { noop, trueToNull } from './utils'

export type OnReceivedCallback = (end?: pull.EndOrError) => void
export type OnReadCallback<In> = (end?: pull.EndOrError, data?: In) => void

export interface DuplexOptions<In, Out> {
  allowHalfOpen: boolean
  abortEagerly: boolean
  onReceived: (this: PushableDuplex<In, Out>, data: Out, cb: OnReceivedCallback) => void
  onRead: (this: PushableDuplex<In, Out>, cb: OnReadCallback<In>) => void
  onSent: (data: In) => void
  onFinished: (err: pull.EndOrError) => void
}

export class PushableDuplex<In, Out> implements pull.Duplex<In, Out> {
  private _source: pull.Source<In> | null = null
  private _sink: pull.Sink<Out> | null = null
  private _rawSinkRead: pull.Source<Out> | null = null

  protected readonly sourceCbs: pull.SourceCallback<In>[] = []

  public readonly buffer: In[] = []
  public readonly sourceState: SourceState
  public readonly sinkState: SinkState

  constructor(private _opts: Partial<DuplexOptions<In, Out>> = {}) {
    _opts.allowHalfOpen = _opts.allowHalfOpen ?? false
    _opts.abortEagerly = _opts.abortEagerly ?? false
    this.sourceState = new SourceState({
      onEnd: this.finish.bind(this),
    })

    this.sinkState = new SinkState({
      onEnd: this.finish.bind(this),
    })
  }

  get source() {
    if (!this._source) {
      const self = this
      this._source = function (abort: pull.Abort, cb: pull.SourceCallback<In>) {
        if (self.sourceState.finished) {
          return cb(self.sourceState.finished)
        }

        self.sourceCbs.push(cb)

        if (abort) {
          self.sourceState.askAbort(abort)
        } else {
          if (self._opts.onRead) {
            self._opts.onRead.call(self, (end, data) => {
              if (end) {
                self.sourceState.askEnd(end)
              } else if (typeof data !== 'undefined') {
                self.push(data)
              }
              self.sourceDrain()
            })
            return
          }
        }
        self.sourceDrain()
      }
    }
    return this._source
  }

  get sink() {
    if (!this._sink) {
      const self = this
      this._sink = function (read) {
        self._rawSinkRead = read
        if (!self.sinkState.normal) return

        self._rawSinkRead(self.sinkState.finishing || self.sinkState.finished, function next(
          end,
          data
        ) {
          if (end) {
            if (self.sinkState.ended(end)) {
              if (!self._opts.allowHalfOpen) {
                self._opts.abortEagerly ? self.abortSource(end) : self.endSource(end)
              }
            }
            return
          }
          if (self._opts.onReceived) {
            if (self._opts.onReceived.length === 1) {
              // no cb provided, go the sync way
              self._opts.onReceived.call(self, data!, noop)
              self._rawSinkRead!(self.sinkState.finishing || self.sinkState.finished, next)
            } else {
              self._opts.onReceived.call(self, data!, (end) => {
                if (end) {
                  self.sinkState.askEnd(end)
                }
                self._rawSinkRead!(self.sinkState.finishing || self.sinkState.finished, next)
              })
            }
          }
        })
      }
    }
    return this._sink
  }

  end(end?: pull.EndOrError) {
    this.endSource(end)
  }

  abort(end?: pull.EndOrError) {
    this.abortSource(end)
  }

  sourceDrain() {
    if (this.sourceState.aborting) {
      const end = this.sourceState.aborting
      // call of all waiting callback functions
      while (this.sourceCbs.length > 0) {
        const cb = this.sourceCbs.shift()
        cb?.(end)
      }
      this.sourceCbs.length = 0
      this.buffer.length = 0

      this.sourceState.ended(end)
      if (!this._opts.allowHalfOpen) {
        this._opts.abortEagerly ? this.abortSink(end) : this.endSink(end)
      }
      return
    }

    while (this.buffer.length > 0) {
      const cb = this.sourceCbs.shift()
      if (cb) {
        const data = this.buffer.shift()!
        cb(null, data)
        if (this._opts.onSent) {
          this._opts.onSent(data)
        }
      } else {
        break
      }
    }

    if (this.sourceState.ending) {
      if (this.buffer.length > 0) return

      const end = this.sourceState.ending

      // call of all waiting callback functions
      while (this.sourceCbs.length > 0) {
        const cb = this.sourceCbs.shift()
        cb?.(end)
      }
      this.sourceCbs.length = 0
      this.sourceState.ended(end)
      if (!this._opts.allowHalfOpen) {
        this._opts.abortEagerly ? this.abortSink(end) : this.endSink(end)
      }
    }
  }

  push(data: In, toHead = false) {
    if (!this.sourceState.normal) return false

    if (toHead) {
      this.buffer.unshift(data)
    } else {
      this.buffer.push(data)
    }
    this.sourceDrain()
    return true
  }

  private abortSource(end: pull.EndOrError = true) {
    if (!this.sourceState.askAbort(end)) return
    this.sourceDrain()

    if (!this._opts.allowHalfOpen) {
      this._opts.abortEagerly ? this.abortSink(end) : this.endSink(end)
    }
  }

  private endSource(end: pull.EndOrError = true) {
    if (!this.sourceState.askEnd(end)) return
    this.sourceDrain()

    if (!this._opts.allowHalfOpen) {
      this.endSink(end)
    }
  }

  private abortSink(abort: pull.Abort = true) {
    if (!this.sinkState.askAbort(abort)) return

    const cont = (end: pull.Abort) => {
      this.sinkState.ended(end)
      if (!this._opts.allowHalfOpen) {
        this._opts.abortEagerly ? this.abortSource(end) : this.endSource(end)
      }
    }

    if (this._rawSinkRead) {
      this._rawSinkRead(abort, (end) => {
        cont(end)
      })
    } else {
      cont(abort)
    }
  }

  private endSink(end: pull.EndOrError = true) {
    if (!this.sinkState.askEnd(end)) return

    this.sinkState.ended(end)
    if (!this._opts.allowHalfOpen) {
      this._opts.abortEagerly ? this.abortSource(end) : this.endSource(end)
    }
  }

  private finish() {
    if (this.sourceState.finished && this.sinkState.finished) {
      const err = trueToNull(this.sourceState.finished) || trueToNull(this.sinkState.finished)
      this._opts.onFinished?.(err)
    }
  }
}
