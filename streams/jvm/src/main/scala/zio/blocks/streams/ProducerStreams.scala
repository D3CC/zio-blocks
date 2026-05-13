/*
 * Copyright 2024-2026 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.blocks.streams

import zio.blocks.streams.internal.StreamError
import zio.blocks.streams.io.Reader

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A push-based sink that a producer uses to emit elements into a stream.
 * Thread-safe: multiple threads may call [[emit]], but [[end]] and [[fail]]
 * should be called at most once (subsequent calls are no-ops).
 *
 * '''Contract:''' Producers MUST call either [[end]] or [[fail]] when done.
 * Failing to do so (e.g. due to an uncaught exception on the producer thread)
 * will cause the consumer to block indefinitely. Use
 * [[ProducerStreams.fromProducerSimple]] for automatic lifecycle management.
 *
 * @note
 *   [[emit]] does not accept `null` values; passing `null` throws
 *   [[NullPointerException]].
 */
trait ProducerSink[-A, -E] {

  /**
   * Emit a single element. Returns `false` if the stream is closed/cancelled.
   *
   * @throws NullPointerException
   *   if `a` is `null`
   */
  def emit(a: A): Boolean

  /** Signal normal completion. Idempotent — second call is a no-op. */
  def end(): Unit

  /** Signal a typed error. Idempotent — second call is a no-op. */
  def fail(error: E): Unit
}

/**
 * JVM-only stream constructor that bridges a push-based producer into a
 * pull-based [[Stream]]. Uses a bounded
 * [[java.util.concurrent.ArrayBlockingQueue]] internally.
 */
object ProducerStreams {

  /**
   * Creates a stream from a push-based producer. The `register` callback
   * receives a [[ProducerSink]] and returns a cancel callback (`() => Unit`)
   * that is invoked when the consumer closes the stream.
   *
   * If `register` throws synchronously, the exception is delivered to the
   * consumer as a stream error (the consumer will not deadlock).
   *
   * @param register
   *   Called during stream compilation. Receives a sink; returns a cancel
   *   callback.
   * @param knownLength
   *   Optional metadata hint for the number of elements.
   * @param bufferSize
   *   Internal queue capacity (default 256). Must be positive.
   */
  def fromProducer[E, A](
    register: ProducerSink[A, E] => () => Unit,
    knownLength: Option[Long] = None,
    bufferSize: Int = 256
  ): Stream[E, A] = {
    require(bufferSize > 0, s"bufferSize must be positive, got: $bufferSize")
    val theKnownLength = knownLength
    new Stream.FromReader[E, A](
      () => {
        val reader = new ProducerReader[A](bufferSize)
        try {
          val cancel = register(reader.sink)
          reader.setCancelCallback(cancel)
        } catch {
          case e: Throwable =>
            reader.sink.fail(e)
        }
        reader
      },
      "ProducerStreams.fromProducer(...)"
    ) {
      override def knownLength: Option[Long] = theKnownLength
    }
  }

  /**
   * Convenience wrapper around [[fromProducer]] that manages the producer
   * lifecycle automatically: runs `produce` on a new daemon thread, calls
   * [[ProducerSink.end]] on normal return, and [[ProducerSink.fail]] if
   * `produce` throws. Cancellation interrupts the producer thread.
   *
   * Use this instead of [[fromProducer]] when custom thread management is not
   * needed.
   */
  def fromProducerSimple[E, A](
    produce: ProducerSink[A, E] => Unit,
    knownLength: Option[Long] = None,
    bufferSize: Int = 256
  ): Stream[E, A] =
    fromProducer[Any, A](
      register = { sink =>
        val thread = new Thread(() => {
          try {
            produce(sink.asInstanceOf[ProducerSink[A, E]])
            sink.end()
          } catch {
            case e: Throwable => sink.fail(e)
          }
        })
        thread.setDaemon(true)
        thread.start()
        () => thread.interrupt()
      },
      knownLength = knownLength,
      bufferSize = bufferSize
    ).asInstanceOf[Stream[E, A]]

  // -- Internal sentinels --

  private val EndMarker: AnyRef = new AnyRef {
    override def toString: String = "EndMarker"
  }

  private final class FailMarker(val error: Any) {
    override def toString: String = s"FailMarker($error)"
  }

  // Poison pill offered to unblock a producer stuck on queue.put()
  private val PoisonPill: AnyRef = new AnyRef {
    override def toString: String = "PoisonPill"
  }

  /**
   * A [[Reader]] backed by an [[java.util.concurrent.ArrayBlockingQueue]]. The
   * producer pushes elements via the associated [[ProducerSink]]; the consumer
   * pulls via `read()`.
   */
  private final class ProducerReader[A](bufferSize: Int) extends Reader[A] {

    private val queue: ArrayBlockingQueue[AnyRef]    = new ArrayBlockingQueue[AnyRef](bufferSize)
    private val terminated: AtomicBoolean            = new AtomicBoolean(false)
    private val cancelled: AtomicBoolean             = new AtomicBoolean(false)
    @volatile private var cancelCallback: () => Unit = null
    @volatile private var _closed: Boolean           = false

    def setCancelCallback(cb: () => Unit): Unit = cancelCallback = cb

    val sink: ProducerSink[A, Any] = new ProducerSink[A, Any] {
      def emit(a: A): Boolean = {
        if (a == null) throw new NullPointerException("ProducerSink.emit does not accept null values")
        if (terminated.get() || cancelled.get()) return false
        try {
          queue.put(a.asInstanceOf[AnyRef])
          // Check again after put — consumer may have cancelled while we were blocked
          if (cancelled.get()) {
            queue.remove(a.asInstanceOf[AnyRef]) // best-effort drain
            false
          } else {
            true
          }
        } catch {
          case _: InterruptedException =>
            Thread.currentThread().interrupt()
            false
        }
      }

      def end(): Unit =
        if (terminated.compareAndSet(false, true)) {
          try queue.put(EndMarker)
          catch { case _: InterruptedException => Thread.currentThread().interrupt() }
        }

      def fail(error: Any): Unit =
        if (terminated.compareAndSet(false, true)) {
          try queue.put(new FailMarker(error))
          catch { case _: InterruptedException => Thread.currentThread().interrupt() }
        }
    }

    def isClosed: Boolean = _closed

    def read[A1 >: A](sentinel: A1): A1 = {
      if (_closed) return sentinel
      try {
        val item = queue.take()
        item match {
          case EndMarker =>
            _closed = true
            sentinel
          case fm: FailMarker =>
            _closed = true
            throw new StreamError(fm.error)
          case _ if item eq PoisonPill =>
            // Consumer cancelled — we got woken up by poison pill
            _closed = true
            sentinel
          case _ =>
            item.asInstanceOf[A1]
        }
      } catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
          _closed = true
          sentinel
      }
    }

    def close(): Unit =
      if (!_closed) {
        _closed = true
        if (cancelled.compareAndSet(false, true)) {
          val cb = cancelCallback
          queue.clear()
          queue.offer(PoisonPill)
          if (cb != null) {
            try cb()
            catch { case _: Exception => () }
          }
        }
      }
  }
}
