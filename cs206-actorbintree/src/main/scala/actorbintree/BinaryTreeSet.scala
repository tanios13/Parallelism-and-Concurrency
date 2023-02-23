/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor.*
import scala.collection.immutable.Queue

object BinaryTreeSet:

  trait Operation:
    def requester: ActorRef
    def id: Int
    def elem: Int

  trait OperationReply:
    def id: Int

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply



class BinaryTreeSet extends Actor:
  import BinaryTreeSet.*
  import BinaryTreeNode.*

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional (used to stash incoming operations during garbage collection)
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case op: Operation => root ! op

    case GC =>
      val newRoot = createRoot
      context.become(garbageCollecting(newRoot), false)
      root ! CopyTo(newRoot)
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case CopyFinished =>
      //TODO - debug why below logic does not work!!
//      root = newRoot
//      context.unbecome()
//      pendingQueue.foreach { root.forward(_) }
//      pendingQueue = Queue.empty
      pendingQueue.foreach { newRoot ! _ }
      pendingQueue = Queue.empty
      root = newRoot
      context.unbecome()

    case op: Operation =>
      pendingQueue = pendingQueue.enqueue(op)

    case GC => /* ignore GC while garbage collection */
  }


object BinaryTreeNode:
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  /**
   * Acknowledges that a copy has been completed. This message should be sent
   * from a node to its parent, when this node and all its children nodes have
   * finished being copied.
   */
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor:
  import BinaryTreeNode.*
  import BinaryTreeSet.*

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive =  { 
    case Insert(requester, id, em) => {
      // log.debug("Starting BinaryTreeNode.Insert id=" + id + " em=" + em)
      if (em == elem) {
        if (removed) {
          removed = false
        }
        requester ! OperationFinished(id)
        // log.debug("Already inserted node id=" + id + " em=" + em)
      } else if (em < elem) {
        if (subtrees contains Left) {
          val leftTree = subtrees(Left)
          leftTree ! Insert(requester, id, em)
        } else {
          val leftNode = context.actorOf(BinaryTreeNode.props(em, false))
          subtrees += (Left -> leftNode)
          requester ! OperationFinished(id)
          // log.debug("Inserted left node id=" + id + " em=" + em)
        }
      } else {
        if (subtrees contains Right) {
          val rightTree = subtrees(Right)
          rightTree ! Insert(requester, id, em)
        } else {
          val rightNode = context.actorOf(BinaryTreeNode.props(em, false))
          subtrees += (Right -> rightNode)
          requester ! OperationFinished(id)
          // log.debug("Inserted right node=" + id + " em=" + em)
        }
      }
    }

    case Contains(requester, id, em) => {
      // log.debug("Starting BinaryTreeNode.Contains id=" + id + " em=" + em)
      if (em == elem) {
        if (removed) {
          requester ! ContainsResult(id, false)
          // log.debug("Node not found on id=" + id + " em=" + em)
        } else {
          requester ! ContainsResult(id, true)
          // log.debug("Found node id=" + id + " em=" + em)
        }
      } else if (em < elem) {
        if (subtrees contains Left) {
          val leftTree = subtrees(Left)
          leftTree ! Contains(requester, id, em)
        } else {
          requester ! ContainsResult(id, false)
          // log.debug("Node not found on left id=" + id + " em=" + em)
        }
      } else {
        if (subtrees contains Right) {
          val rightTree = subtrees(Right)
          rightTree ! Contains(requester, id, em)
        } else {
          requester ! ContainsResult(id, false)
          // log.debug("Node not found on right id=" + id + " em=" + em)
        }
      }
    }

    case Remove(requester, id, em) => {
      // log.debug("Starting BinaryTreeNode.Remove id=" + id + " em=" + em)
      if (em == elem) {
        if (!removed) {
          removed = true
        }
        requester ! OperationFinished(id)
        // log.debug("Removed node id=" + id + " em=" + em)
      } else if (em < elem) {
        if (subtrees contains Left) {
          val leftTree = subtrees(Left)
          leftTree ! Remove(requester, id, em)
        } else {
          requester ! OperationFinished(id)
          // log.debug("Missing left node id=" + id + " em=" + em)
        }
      } else {
        if (subtrees contains Right) {
          val rightTree = subtrees(Right)
          rightTree ! Remove(requester, id, em)
        } else {
          requester ! OperationFinished(id)
          // log.debug("Missing right node id=" + id + " em=" + em)
        }
      }
    }

    case CopyTo(treeNode) => {
      if (removed && subtrees.isEmpty) {
        context.parent ! CopyFinished
        context.stop(self)
        // log.debug("Do not copy removed leaf node elem=" + elem)
      } else {
        var expected = Set[ActorRef]()
        if (subtrees contains Left) {
          expected += subtrees(Left)
        }
        if (subtrees contains Right) {
          expected += subtrees(Right)
        }
      
        if (removed) {
          context.become(copying(expected, true))
          // log.debug("Copy subtrees of removed node elem=" + elem)
        } else {
          context.become(copying(expected, false))
          treeNode ! Insert(self, 0, elem)
          // log.debug("Copy node with subtrees elem=" + elem)
        }
        subtrees.values foreach {_ ! CopyTo(treeNode)}
      }
    }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(_) => {
      if (expected.isEmpty) {
        context.parent ! CopyFinished
        context.stop(self)
        // log.debug("CopyFinished after insert")
      } else {
        context.become(copying(expected, true))
      }
    }
    case CopyFinished => {
      val waiting = expected - sender()
      if (waiting.isEmpty && insertConfirmed) {
        context.parent ! CopyFinished
        context.stop(self)
        // log.debug("CopyFinished after copying subtrees")
      } else {
        context.become(copying(waiting, insertConfirmed))
      }
    }
  }


