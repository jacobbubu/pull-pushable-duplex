import checker from '@jacobbubu/pull-stream-protocol-checker'
import { OnReadCallback } from '../src'

export function getProbe() {
  return checker({ forbidExtraRequests: true, enforceStreamTermination: true, notifyEagerly: true })
}

export function valuesToRead<T>(values: T[] = [], delay = 0) {
  let i = 0
  return (cb: OnReadCallback<T, any>) => {
    if (delay <= 0) {
      i === values.length ? cb(true) : cb(null, values[i])
      i += 1
    } else {
      setTimeout(() => {
        i === values.length ? cb(true) : cb(null, values[i])
        i += 1
      }, delay)
    }
  }
}
