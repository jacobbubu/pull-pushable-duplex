import * as pull from 'pull-stream'
import { SourceState } from './source-state'
import { SinkState } from './sink-state'
import { noop, trueToNull } from './utils'

export type OnReceivedCallback = (end?: pull.EndOrError) => void
export type OnReadCallback<In, State> = (end?: pull.EndOrError, data?: In, newState?: State) => void
export type OnReadAbortCallback<State> = (newState?: State) => void
export type OnRawReadCallback = () => void

export interface DuplexOptions<In, Out, State> {
  allowHalfOpen: boolean
  abortEagerly: boolean
  autoStartReading: boolean
  initialState: State
  onReceived: (this: PushableDuplex<In, Out, State>, data: Out, cb: OnReceivedCallback) => void
  onRead: (
    this: PushableDuplex<In, Out, State>,
    cb: OnReadCallback<In, State>,
    state: State | undefined
  ) => void
  onReadAbort: (err: pull.Abort, cb: OnReadAbortCallback<State>, state: State | undefined) => void
  onRawReadEnd: (err: pull.EndOrError, cb: OnRawReadCallback) => void
  onSent: (data: In) => void
  onSourceEnded: (err: pull.EndOrError) => void
  onSinkEnded: (err: pull.EndOrError) => void
  onFinished: (err: pull.EndOrError) => void
}

export class PushableDuplex<In, Out, State> implements pull.Duplex<In, Out> {
  private _source: pull.Source<In> | null = null
  private _sink: pull.Sink<Out> | null = null
  private _rawSinkRead: pull.Source<Out> | null = null
  private _reentered = 0
  private _state: State | undefined = undefined
  private _autoStartReading: boolean
  private _readStarted: boolean
  private _onSourceEndedCalled = false
  private _onSinkEndedCalled = false

  protected readonly sourceCbs: pull.SourceCallback<In>[] = []

  public readonly buffer: In[] = []
  public readonly sourceState: SourceState
  public readonly sinkState: SinkState

  constructor(private _opts: Partial<DuplexOptions<In, Out, State>> = {}) {
    _opts.allowHalfOpen = _opts.allowHalfOpen ?? false
    _opts.abortEagerly = _opts.abortEagerly ?? false
    this._state = _opts.initialState
    this._autoStartReading = _opts.autoStartReading ?? true
    this._readStarted = false

    this.sourceState = new SourceState({
      onEnd: this.finish.bind(this),
    })

    this.sinkState = new SinkState({
      onEnd: this.finish.bind(this),
    })
  }

  startReading() {
    if (this._rawSinkRead) {
      this.readLoop()
    }
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
          if (self.sourceState.askAbort(abort)) {
            if (self._opts.onReadAbort) {
              self._opts.onReadAbort.call(
                self,
                abort,
                (newState) => {
                  if (newState !== undefined) {
                    self._state = newState
                  }
                  self.sourceDrain()
                },
                self._state
              )
              return
            }
          }
        } else {
          if (self._opts.onRead) {
            self._opts.onRead.call(
              self,
              (end, data, newState) => {
                if (end) {
                  self.sourceState.askEnd(end)
                  self.sourceDrain()
                } else if (typeof data !== 'undefined') {
                  if (newState !== undefined) {
                    self._state = newState
                  }
                  self.push(data)
                }
              },
              self._state
            )
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
        if (self._autoStartReading) {
          self.readLoop()
        }
      }
    }
    return this._sink
  }

  get readStarted() {
    return this._readStarted
  }

  end(end?: pull.EndOrError) {
    this.endSource(end)
    this.endSink(end)
  }

  abort(end?: pull.EndOrError) {
    this.abortSource(end)
    this.abortSink(end)
  }

  sourceDrain() {
    if (this.drainAbort()) return

    this.drainNormal()
    if (this.drainAbort()) return

    this.drainEnd()
    if (this.drainAbort()) return
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

  private readLoop() {
    if (this._readStarted) return
    this._readStarted = true

    if (!this.sinkState.normal) return

    const self = this
    this._rawSinkRead!(self.sinkState.finishing || self.sinkState.finished, function next(
      end,
      data
    ) {
      if (end) {
        if (self._opts.onRawReadEnd) {
          self._opts.onRawReadEnd(end, () => {
            if (self.sinkState.ended(end)) {
              if (!self._opts.allowHalfOpen) {
                self._opts.abortEagerly ? self.abortSource(end) : self.endSource(end)
              }
            }
          })
        } else {
          if (self.sinkState.ended(end)) {
            if (!self._opts.allowHalfOpen) {
              self._opts.abortEagerly ? self.abortSource(end) : self.endSource(end)
            }
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

  private drainAbort() {
    if (!this.sourceState.aborting || this._reentered > 0) return false
    this._reentered++

    try {
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
    } finally {
      this._reentered--
    }
    return true
  }

  private drainNormal() {
    if (this._reentered > 0) return

    this._reentered++
    try {
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
    } finally {
      this._reentered--
    }
  }

  private drainEnd() {
    if (!this.sourceState.ending || this._reentered > 0) return
    this._reentered++

    try {
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
    } finally {
      this._reentered--
    }
  }

  abortSource(end: pull.EndOrError = true) {
    if (!this.sourceState.askAbort(end)) return
    this.sourceDrain()

    if (!this._opts.allowHalfOpen) {
      this._opts.abortEagerly ? this.abortSink(end) : this.endSink(end)
    }
  }

  endSource(end: pull.EndOrError = true) {
    if (!this.sourceState.askEnd(end)) return
    this.sourceDrain()

    if (!this._opts.allowHalfOpen) {
      this.endSink(end)
    }
  }

  abortSink(abort: pull.Abort = true) {
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

  endSink(end: pull.EndOrError = true) {
    if (!this.sinkState.askEnd(end)) return

    this.sinkState.ended(end)
    if (!this._opts.allowHalfOpen) {
      this._opts.abortEagerly ? this.abortSource(end) : this.endSource(end)
    }
  }

  private finish() {
    if (this.sourceState.finished && this._opts.onSourceEnded && !this._onSourceEndedCalled) {
      this._onSourceEndedCalled = true
      this._opts.onSourceEnded(this.sourceState.finished)
    }

    if (this.sinkState.finished && this._opts.onSinkEnded && !this._onSinkEndedCalled) {
      this._onSinkEndedCalled = true
      this._opts.onSinkEnded(this.sinkState.finished)
    }

    if (this.sourceState.finished && this.sinkState.finished) {
      const err = trueToNull(this.sourceState.finished) || trueToNull(this.sinkState.finished)
      this._opts.onFinished?.(err)
    }
  }
}
