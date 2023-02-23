package hazardptr

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import instrumentation.Monitor

import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.tailrec

class HazardPointer(numPointerPerThread: Int) extends AbstractHazardPointer(numPointerPerThread):

  def update(i: Int, n: Option[Node]): Unit =
    hazardPointerArray(getMyId() + i) = n


  def get(i: Int): Option[Node] =
    hazardPointerArray(getMyId() + i)

  def retireNode(node: Option[Node]): Unit =
    val retiredList = retiredListArray(getMyId())
    retiredList += node
    if(retiredList.size > threshold){

      var i = 0
      while(i < retiredList.size){
        if(!isHazard(retiredList(i), 0)){
          retiredList.remove(i)
        }
        i += 1
      }
    }

    @tailrec
    def isHazard(node: Option[Node], i: Int): Boolean ={
      if (i >= hazardPointerArray.size) false
      else
        if (hazardPointerArray(i) == node) true
        else isHazard(node, i+1)
    }