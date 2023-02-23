package hazardptr.instrumentation

import scala.annotation.tailrec
import hazardptr._
import java.util.concurrent.atomic._

class SchedulableNode(value: Int,_next: Option[Node],_prev: Option[Node], val scheduler: Scheduler) extends Node(value, _next, _prev) with MockedMonitor { self =>

  override def toString: String =
    String.format("Node(%s)#%02d", value.toString, ## % 100)

  /*override def next:Option[Node] = scheduler.exec {_next}(s"read next_pointer")
  override def next_=(e: Option[Node]): Unit = scheduler.exec {_next = if(e.isDefined) e else None}(s"update next_pointer")
  override def prev:Option[Node] = scheduler.exec {_prev }(s"read prev_pointer")
  override def prev_=(e: Option[Node]): Unit = scheduler.exec {_prev = if(e.isDefined) e else None}(s"update prev_pointer")*/
  //override def =(e: Option[Node]): Unit = scheduler.exec(value = e.get.value; _next = e.get.next; _prev = e.get.prev)(s"Copy")
}

class SchedulableHazardPointer(numPointerPerThread: Int, val scheduler: Scheduler) extends HazardPointer(numPointerPerThread) with MockedMonitor { self =>

  override def toString: String = "Hazard Pointer"

  override val threshold = scheduler.numThreads * numPointerPerThread + 1

  override def getMyId():Int =
    scheduler.threadId
}

class SchedulableSortedList(val scheduler: Scheduler) extends SortedList with MockedMonitor:

  override def createNode(value: Int, next: Option[Node], prev: Option[Node], isHead: Boolean) = new SchedulableNode(value, next, prev, scheduler) { self =>
    override def toString = if isHead then "HEAD" else super.toString
  }

  override val hazardPointers = new SchedulableHazardPointer(2, scheduler)

  override def insert(e: Int): Unit =
    scheduler.exec {
      super.insert(e)
    }(s"Insert")

  override def delete(e: Int): Boolean =
    scheduler.exec {
      super.delete(e)
    }(s"Delete")

