package edu.berkeley.cs.scads
package storage

import edu.berkeley.cs.scads.comm._


import java.util.concurrent.TimeUnit

/**
 * Simple helper trait for parallel future handling
 */
trait ParFuture {
  /**
   * Blocks until each future in ftchs has been collected. Upon collection,
   * the result of the future is applied to f, and the results are collected
   * and returned.
   */
  def waitFor[Data, T](ftchs: Seq[(MessageFuture, Data)], timeout: Long = 5000)(f: PartialFunction[(MessageBody, Data), T]): Seq[Either[Throwable, T]] = {
    def trapException(elem: (MessageFuture, Data)): Either[Throwable, (MessageBody, Data)] = 
      try {
        elem._1
          .get(timeout, TimeUnit.MILLISECONDS)
          .map(e => Right((e, elem._2)))
          .getOrElse(Left(new RuntimeException("TIMEOUT")))
      } catch {
        case e => Left(e)
      }
    ftchs.map(ftch => 
      trapException(ftch).fold(
        ex => Left(ex), 
        msg => f.lift(msg).map(Right(_)).getOrElse(Left(new RuntimeException("Unexpected message during get")))))
  }


  def waitForAndThrowException[Data, T](ftchs: Seq[(MessageFuture, Data)], timeout: Long = 5000)(f: PartialFunction[(MessageBody, Data), T]): Seq[T] = 
    waitFor(ftchs)(f).map {
      case Left(ex) => throw ex
      case Right(t) => t
    }
}
