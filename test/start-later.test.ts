import * as pull from 'pull-stream'
import { PushableDuplex } from '../src'
import { getProbe, valuesToRead } from './common'

describe('start later', () => {
  it('ex1', (done) => {
    const startTime = Date.now()
    const waitFor = 500

    const s1Values = [1, 2, 3]
    const s2Values = ['a', 'b', 'c']
    const results: any[] = []
    const d = new PushableDuplex({
      allowHalfOpen: true,
      autoStartReading: false,
      onRead: valuesToRead(s1Values),
      onReceived: (data) => {
        results.push(data)
      },
      onFinished: (err) => {
        expect(err).toBeNull()
        expect(results).toEqual(s2Values)
        const elapsed = Date.now() - startTime
        expect(elapsed).toBeGreaterThan((waitFor / 5) * 4)
        done()
      },
    })

    const peer = {
      source: pull(pull.values(s2Values), getProbe()),
      sink: pull(
        getProbe(),
        pull.collect((err, results) => {
          expect(err).toBeFalsy()
          expect(results).toEqual(s1Values)
        })
      ),
    }
    pull(d, peer, d)

    setTimeout(() => {
      d.startReading()
    }, waitFor)
  })
})
