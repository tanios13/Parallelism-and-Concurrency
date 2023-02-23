package pubsub.collection

class BoundedBuffer[T](size: Int) extends AbstractBoundedBuffer[T](size):

  // You have at your disposition the following two variables:
  // - count : Int
  // - head : Int
  // In addition, you have access to an array-like internal buffer:
  // - buffer
  // You can access elements of this buffer using:
  // - buffer(i)
  // Similarly, you can set elements using:
  // - buffer(i) = e
  //
  // You do not need to create those variables yourself!
  // They are inherited from the AbstractBoundedBuffer class.

  override def put(e: T): Unit = synchronized {
    while(isFull)
      wait()
    
    buffer(indexOfTail) = e
    count += 1
    notifyAll()
  }

  override def take(): T = synchronized {
    while(isEmpty) 
      wait()
    
    val e: T = buffer(head)
    head = (head + 1) % size
    count -= 1
    notifyAll()
    e
  }

  // You may want to add methods to:
  // - check whether the buffer is empty
  def isEmpty: Boolean = count == 0

  // - check whether the buffer is full
  def isFull: Boolean = size == count

  // - get the index of tail
  def indexOfTail: Int = (head + count) % size
