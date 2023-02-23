package hazardptr

import scala.concurrent._
import scala.concurrent.duration._
import scala.collection.mutable.HashMap
import scala.util.Random
import instrumentation.SchedulableSortedList
import instrumentation.TestHelper._
import instrumentation.TestUtils._

class SortedListSuite extends munit.FunSuite:

  test("Should work when 1, 2, and 3 are inserted sequentially") {
    val l = new SortedList
    l.insert(1)
    l.insert(2)
    l.insert(3)
    assertEquals(l.toList, List(1, 2, 3))
    for(i <- 0 to 1)
      assert(l.hazardPointers.hazardPointerArray(i).isDefined)
    for(i <- 2 to 19)
      assert(!l.hazardPointers.hazardPointerArray(i).isDefined)
  }

  test("Should work when 3, 2, and 1 are inserted sequentially") {
    val l = new SortedList
    l.insert(3)
    l.insert(2)
    l.insert(1)
    assertEquals(l.toList, List(1, 2, 3))
  }

  test("Should work when duplicate elements are inserted sequentially") {
    val l = new SortedList
    l.insert(0)
    l.insert(0)
    l.insert(2)
    l.insert(2)
    assertEquals(l.toList, List(0, 0, 2, 2))
  }

  test("Should return [1,4,5] when from [1,2,3,5], a thread removes 3, 2 and then inserts 4") {
    val l = new SortedList
    l.insert(1)
    l.insert(2)
    l.insert(3)
    l.insert(5)
    l.delete(3)
    l.delete(2)
    l.insert(4)
    assertEquals(l.toList, List(1, 4, 5))
    assertEquals(l.hazardPointers.retiredListArray(0).size, 2)
  }

  test("Should return 5 when from [1,2,3,4,5] NthNext is called with element 3 and position 2") {
    val l = new SortedList
    var prev = l._head
    for i <- 1 to 5 do
    {
      val newNode0 = Option(l.createNode(i, None, prev))
      prev.get.next = newNode0
      prev = newNode0
    }
    assertEquals(l.getNthNext(3,2), 5)
    for(i <- 0 to 1)
      assert(l.hazardPointers.hazardPointerArray(i).isDefined)
    for(i <- 2 to 19)
      assert(!l.hazardPointers.hazardPointerArray(i).isDefined)
  }

  test("Should return 5 when countOccurrences(1) is called for list [0,1,1,1,1,1,2]") {
    val l = new SortedList
    var prev = l._head
    val newNode = Option(l.createNode(0, None, prev))
    prev.get.next = newNode
    prev = newNode
    for i <- 1 to 5 do
    {
      val newNode0 = Option(l.createNode(1, None, prev))
      prev.get.next = newNode0
      prev = newNode0
    }
    val newNode2 = Option(l.createNode(2, None, prev))
    prev.get.next = newNode2
    prev = newNode2
    assertEquals(l.countOccurrences(1), 5)
    for(i <- 0 to 1)
      assert(l.hazardPointers.hazardPointerArray(i).isDefined)
    for(i <- 2 to 19)
      assert(!l.hazardPointers.hazardPointerArray(i).isDefined)
  }

  test("Should return 0 when countOccurrences(3) is called for list [0,1,1,1,1,1,2]") {
    val l = new SortedList
    var prev = l._head
    val newNode = Option(l.createNode(0, None, prev))
    prev.get.next = newNode
    prev = newNode
    for i <- 1 to 5 do
    {
      val newNode0 = Option(l.createNode(1, None, prev))
      prev.get.next = newNode0
      prev = newNode0
    }
    val newNode2 = Option(l.createNode(2, None, prev))
    prev.get.next = newNode2
    prev = newNode2
    assertEquals(l.countOccurrences(3), 0)
    for(i <- 0 to 1)
      assert(l.hazardPointers.hazardPointerArray(i).isDefined)
    for(i <- 2 to 19)
      assert(!l.hazardPointers.hazardPointerArray(i).isDefined)
  }

  test("Should work when a random list of 100 elements are inserted sequentially") {
    val rand = new Random(42)
    val randvals = for i <- 1 to 100 yield rand.nextInt()
    println(s"test list = ${randvals.toList}")
    val l = new SortedList
    randvals.foreach { l.insert }
    assertEquals[Any, Any](l.toList, randvals.sorted)
  }

  test("Should insert in parallel 1, 2 and 3 in the list (0, 4)") {
    testManySchedules(3, sched => {
      val sortedList = new SchedulableSortedList(sched)
      val node0 = Option(sortedList.createNode(0, None, sortedList._head))
      sortedList._head.get.next = node0
      val node4 = Option(sortedList.createNode(4, None, node0))
      node0.get.next = node4
      ((for i <- 1 to 3 yield () => sortedList.insert(i)).toList,
       results => {
        val res = sortedList.toList
        (res == List(0, 1, 2, 3, 4),
        s"expected List(0, 1, 2, 3, 4), got $res")
      })
    })
  }

  test("Should return List(true, false) when the first thread deletes 2 and the sec") {
    testManySchedules(2, sched => {
      val sortedList = new SchedulableSortedList(sched)
      val node1 = Option(sortedList.createNode(1, None, sortedList._head))
      sortedList._head.get.next = node1
      val node2 = Option(sortedList.createNode(2, None, node1))
      node1.get.next = node2
      val node3 = Option(sortedList.createNode(3, None, node2))
      node2.get.next = node3
      (List(() => sortedList.delete(2),
            () => sortedList.delete(4)),
      results => {
        val res = sortedList.toList
        val expected = List(1, 3)
        if res != expected then {
           (false, s"expected the final list to be $expected, your 'delete' implementation returned ${res}")
        } else if results != List(true, false) then {
          (false, s"expected threads to return List(true, false), your 'delete' implementation returned ${results}")
        } else (true, "")
      })
    })
  }


  import scala.concurrent.duration._
  override val munitTimeout = 200.seconds
