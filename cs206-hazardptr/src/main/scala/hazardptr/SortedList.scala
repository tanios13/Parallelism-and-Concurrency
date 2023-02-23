package hazardptr

import scala.annotation.tailrec
import util.control.Breaks._

class SortedList extends AbstractSortedList:
  
  // The sentinel node at the head.
  val _head: Option[Node] = Option(createNode(0, None, None, isHead = true))

  // The first logical node is referenced by the head.
  def firstNode: Option[Node] = _head.get.next

  val hazardPointers = new HazardPointer(2)

  // Finds the first node whose value satisfies the predicate.
  // Returns the predecessor of the node and the node.
  def findNodeWithPrev(pred: Int => Boolean): (Option[Node], Option[Node]) =
    @tailrec
    def helper(n1: Option[Node], n2: Option[Node]): (Option[Node], Option[Node]) =
      hazardPointers.update(0, n1)
        n2 match
          case Some(n) =>
            hazardPointers.update(1, n2)
              if (pred(n.value))
                if (n1.get.next == n2) (n1, n2)
                else findNodeWithPrev(pred)
              else helper(n2, n.next)
          case None => (n1, n2)

    helper(_head, firstNode)

  //Find Nth Node after current element. Return 0 if out of bounds.
  def getNthNext(e: Int, n: Int): Int = {
    val (_, next) = findNodeWithPrev(_ == e)

    @tailrec
    def advance(currNode: Option[Node], remaining: Int): Int ={
        currNode match
          case None => 0
          case Some(node) =>
            if (remaining <= 0) node.value
            else advance(node.next, remaining - 1)
    }

    advance(next, n)
  }

  // Count occurrence of the element.
  def countOccurrences(e: Int): Int = {
    @tailrec
    def helper(node: Option[Node], occurrences: Int): Int = {
      node match
        case None => occurrences
        case Some(n) =>
          hazardPointers.update(0, node)
            if (n.next.isDefined) hazardPointers.update(1, n.next)
            if (n.value == e) helper(n.next, occurrences + 1)
            else helper(n.next, occurrences)
    }
    helper(firstNode, 0)
  }

  // Insert an element in the list.
  def insert(e: Int): Unit = synchronized {
    val (prev, next) = findNodeWithPrev(_ >= e)
    val newNode = Option(createNode(e, next, prev))
    prev.get.next = newNode
    next match
      case Some(v) => v.prev = newNode
      case _ =>
  }

  // Checks if the list contains an element.
  def contains(e: Int): Boolean = {
    val (_, next) = findNodeWithPrev(_ == e)
    next.isDefined
  }

  // Delete an element from the list.
  // Should only delete one element when multiple occurrences are present.
  def delete(e: Int): Boolean = synchronized {
    val (prev, next) = findNodeWithPrev(_ == e)
    (prev, next) match
      case (Some(n1), Some(n2)) =>
        n1.next = n2.next
        if (n2.next.isDefined) n2.next.get.prev = prev
        hazardPointers.update(0, None)
        hazardPointers.update(1, None)
        hazardPointers.retireNode(next)
        true
      case _ => false
  }