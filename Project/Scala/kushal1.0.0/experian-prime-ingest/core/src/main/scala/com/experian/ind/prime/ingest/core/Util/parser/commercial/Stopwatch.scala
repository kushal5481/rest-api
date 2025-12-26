package com.experian.ind.prime.ingest.core.Util.parser.commercial

import java.time.{Duration, Instant}
import java.util.concurrent.TimeUnit

final class Stopwatch private (mode: Stopwatch.Mode) {
  
  private val startNanos: Long = if (mode == Stopwatch.Mode.TIMEUNIT) System.nanoTime() else 0L
  private val startInstant: Instant = if (mode == Stopwatch.Mode.DURATION) Instant.now() else null
  
  private var stopped: Boolean = false
  private var stoppedNanos: Long = 0L
  private var stoppedDuration: Duration = null

  def stop(): Unit = {
    if (!stopped) {
      if (mode == Stopwatch.Mode.TIMEUNIT) {
        stoppedNanos = System.nanoTime() - startNanos
      } else {
        stoppedDuration = Duration.between(startInstant, Instant.now())
      }
      stopped = true
    }
  }

  def elapsedFormatted(): String = {
    val (hours, minutes, seconds, millis) = if (mode == Stopwatch.Mode.TIMEUNIT) {
      val nanos = if (stopped) stoppedNanos else System.nanoTime() - startNanos
      val h = TimeUnit.NANOSECONDS.toHours(nanos)
      val m = TimeUnit.NANOSECONDS.toMinutes(nanos) % 60
      val s = TimeUnit.NANOSECONDS.toSeconds(nanos) % 60
      val ms = TimeUnit.NANOSECONDS.toMillis(nanos) % 1000
      (h, m, s, ms)
    } else {
      val d = if (stopped) stoppedDuration else Duration.between(startInstant, Instant.now())
      val h = d.toHours
      val m = getMinutesPart(d)
      val s = getSecondsPart(d)
      val ms = getMillisPart(d)
      (h, m, s, ms)
    }

    // Build adaptive format
    if (hours > 0) {
      f"${hours}%02dh ${minutes}%02dm ${seconds}%02ds ${millis}%03dms"
    } else if (minutes > 0) {
      f"${minutes}%02dm ${seconds}%02ds ${millis}%03dms"
    } else if (seconds > 0) {
      f"${seconds}%02ds ${millis}%03dms"
    } else {
      s"${millis}ms"
    }
  }

  def elapsedMillis(): Long = {
    if (mode == Stopwatch.Mode.TIMEUNIT) {
      TimeUnit.NANOSECONDS.toMillis(if (stopped) stoppedNanos else System.nanoTime() - startNanos)
    } else {
      (if (stopped) stoppedDuration else Duration.between(startInstant, Instant.now())).toMillis
    }
  }
  
  /**
   * Get elapsed time as formatted string (alias for elapsedFormatted)
   */
  def elapsed(): String = elapsedFormatted()

  // Java 9+ helpers with Java 8 fallback
  private def getMinutesPart(d: Duration): Long = {
    try {
      d.toMinutesPart()
    } catch {
      case _: NoSuchMethodError => d.toMinutes % 60
    }
  }

  private def getSecondsPart(d: Duration): Long = {
    try {
      d.toSecondsPart()
    } catch {
      case _: NoSuchMethodError => d.getSeconds % 60
    }
  }

  private def getMillisPart(d: Duration): Long = {
    try {
      d.toMillisPart()
    } catch {
      case _: NoSuchMethodError => d.toMillis % 1000
    }
  }
}

object Stopwatch {
  sealed trait Mode
  object Mode {
    case object TIMEUNIT extends Mode
    case object DURATION extends Mode
  }

  def startNew(mode: Mode): Stopwatch = new Stopwatch(mode)
  
  /**
   * Create and start a new stopwatch with TIMEUNIT mode (default)
   */
  def createStarted(): Stopwatch = startNew(Mode.TIMEUNIT)
  
  /**
   * Create and start a new stopwatch with specified mode
   */
  def createStarted(mode: Mode): Stopwatch = startNew(mode)
}
