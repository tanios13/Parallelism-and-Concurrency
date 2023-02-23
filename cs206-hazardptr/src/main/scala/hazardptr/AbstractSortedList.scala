package hazardptr

import instrumentation.Monitor
import scala.collection._
import java.util.concurrent.atomic._


abstract class AbstractSortedList extends Monitor:
  
  def createNode(value: Int, next: Option[Node], prev: Option[Node], isHead: Boolean = false) = new Node(value, next, prev) {
    override def toString = if isHead then "HEAD" else super.toString
  }
  
  def firstNode: Option[Node] 
  
  def findNodeWithPrev(pred: Int => Boolean): (Option[Node], Option[Node])

  def getNthNext(e: Int, n: Int): Int

  def countOccurrences(e: Int): Int

  def contains(e: Int): Boolean
  
  def insert(e: Int): Unit
  
  def delete(e: Int): Boolean
    
  def toList: List[Int] =
    var curr = firstNode
    var list = List[Int]()
    while curr.nonEmpty do
      list :+= curr.get.value
      curr = curr.get.next 
    list

