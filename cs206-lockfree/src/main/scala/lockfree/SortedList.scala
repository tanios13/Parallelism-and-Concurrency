package lockfree

import scala.annotation.tailrec

class SortedList extends AbstractSortedList:

  // The sentinel node at the head.
  private val _head: Node = createNode(0, None, isHead = true)

  // The first logical node is referenced by the head.
  def firstNode: Option[Node] = _head.next

  // Finds the first node whose value satisfies the predicate.
  // Returns the predecessor of the node and the node.
  def findNodeWithPrev(pred: Int => Boolean): (Node, Option[Node]) = {

      def findRec(previous : Node, current : Option[Node]): (Node, Option[Node]) = {
        
        if(current == None) (previous, None)
        else if(current.get.deleted){
            previous.atomicState.compareAndSet((current, false), (current.get.next, false))
            findNodeWithPrev(pred)
        }
        else if(pred(current.get.value)) (previous, current)
        else findRec(current.get, current.get.next)

      }

      findRec(_head, firstNode)

  }

  // Insert an element in the list.
  def insert(e: Int): Unit = {
    val (pred, current) : (Node, Option[Node]) = findNodeWithPrev(x => x > e)
    val newNode = createNode(e, current)
    if(!pred.atomicState.compareAndSet((current, false), (Some(newNode), false))){
      insert(e)
    }
    
  }

  // Checks if the list contains an element.
  def contains(e: Int): Boolean = {
    val (pred, current) : (Node, Option[Node]) = findNodeWithPrev(_ == e)
    if(current == None) false
    else if(current.get.deleted) false
    else true
  }

  // Delete an element from the list.
  // Should only delete one element when multiple occurences are present.
  def delete(e: Int): Boolean = {
    val (pred, current) = findNodeWithPrev(_ == e)
    if(current == None) false
    else if(!current.get.mark){
      delete(e)
    }
    else true
  }
