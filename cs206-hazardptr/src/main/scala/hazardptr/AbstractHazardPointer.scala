package hazardptr

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import instrumentation.Monitor
import java.util.concurrent.atomic.AtomicInteger

abstract class AbstractHazardPointer(numPointerPerThread: Int) extends Monitor:

  val maxThreads = 10

  val threshold = maxThreads * numPointerPerThread + 1

  val hazardPointerArray: ArrayBuffer[Option[Node]] = ArrayBuffer.fill(numPointerPerThread * maxThreads)(None)

  val retiredListArray: ArrayBuffer[ArrayBuffer[Option[Node]]] = ArrayBuffer.fill(maxThreads)(ArrayBuffer())

  // This function will be overridden when running in parallel.
  def getMyId(): Int = 0

  // Update ith per-thread hazard pointer ( 0 <= i <= numPointerPerThread )
  def update(i: Int, n: Option[Node]): Unit

  // Return ith per-thread hazard pointer ( 0 <= i <= numPointerPerThread )
  def get(i: Int): Option[Node]

  def retireNode(node: Option[Node]) : Unit
