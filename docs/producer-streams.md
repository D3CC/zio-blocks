# Producer-Backed Streams (JVM)

## Overview

`ProducerStreams.fromProducer` bridges a push-based producer (running on any thread) into a
pull-based `Stream[E, A]`. The producer pushes elements via a `ProducerSink`; the consumer
pulls them through the stream interface. A bounded `ArrayBlockingQueue` sits between the two,
so the producer blocks when the buffer is full and the consumer blocks when it's empty.

## API

```scala
package zio.blocks.streams

// Push-based sink handed to the producer.
trait ProducerSink[-A, -E] {
  def emit(a: A): Boolean    // false if stream is closed/cancelled
  def end(): Unit            // signal normal completion; idempotent
  def fail(error: E): Unit   // signal typed error; idempotent
}

// JVM-only constructors (streams/jvm/).
object ProducerStreams {
  // Full control: user manages thread + cancel callback.
  def fromProducer[E, A](
    register: ProducerSink[A, E] => () => Unit,
    knownLength: Option[Long] = None,
    bufferSize: Int = 256
  ): Stream[E, A]

  // Managed lifecycle: auto-end, auto-fail on exception, daemon thread.
  def fromProducerSimple[E, A](
    produce: ProducerSink[A, E] => Unit,
    knownLength: Option[Long] = None,
    bufferSize: Int = 256
  ): Stream[E, A]
}
```

`register` is called once when the stream is compiled. It receives a `ProducerSink`, starts
the producer (typically on a separate thread), and returns a cancel callback `() => Unit`.

## Usage Example

```scala
import zio.blocks.streams.{ProducerStreams, ProducerSink}

val stream: Stream[Nothing, Int] = ProducerStreams.fromProducer { sink =>
  val thread = new Thread(() => {
    (1 to 10).foreach { i =>
      if (!sink.emit(i)) return // consumer cancelled
    }
    sink.end()
  })
  thread.start()
  () => thread.interrupt() // cancel callback
}
```

## Lifecycle Contract

| Producer action | While active | After terminal |
|-----------------|-------------|----------------|
| `emit(a)`       | enqueues, returns `true` | returns `false` |
| `end()`         | signals completion | no-op |
| `fail(e)`       | signals error | no-op |
| consumer close  | cancel callback fired | no-op |

- `end` and `fail` are idempotent; the first call wins.
- `emit` after any terminal state returns `false` — the producer should treat this as a signal
  to stop work.
- The cancel callback is invoked exactly once, on the consumer thread, during early termination.
  It is **not** called when the stream ends normally via `end()`.

## Known-Length

Pass `knownLength = Some(n)` to expose element count as metadata:

```scala
ProducerStreams.fromProducer(register, knownLength = Some(fileSize))
```

This is a hint only. The runtime does not verify that exactly `n` elements are emitted.
`map` preserves it; `filter` clears it (standard `Stream` semantics).

## Threading Model

- **Producer thread**: pushes elements via `ProducerSink`. Blocks on `queue.put` when the
  buffer is full.
- **Consumer thread**: pulls elements via the stream. Blocks on `queue.take` when the buffer
  is empty.
- On consumer cancel, a poison pill is offered to unblock a producer stuck in `queue.put`.

## HTTP Integration

To back a `Body` (or similar HTTP response type) with a producer-driven byte stream:

```scala
// zio-http Body wraps Stream[Nothing, Byte]
val byteStream: Stream[Nothing, Byte] = ProducerStreams.fromProducer(
  register = sink => {
    val thread = new Thread(() => {
      readChunksFromSource { chunk =>
        chunk.foreach { b => if (!sink.emit(b)) return }
      }
      sink.end()
    })
    thread.start()
    () => thread.interrupt()
  },
  knownLength = contentLength
)

Body.fromStream(byteStream)
```

The `knownLength` maps directly to the `Content-Length` header when the body size is known
ahead of time.
