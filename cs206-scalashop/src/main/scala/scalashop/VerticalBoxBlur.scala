package scalashop

import org.scalameter.*

object VerticalBoxBlurRunner:

  val standardConfig = config(
    Key.exec.minWarmupRuns := 5,
    Key.exec.maxWarmupRuns := 10,
    Key.exec.benchRuns := 10,
    Key.verbose := false
  ) withWarmer(Warmer.Default())

  def main(args: Array[String]): Unit =
    val radius = 3
    val width = 1920
    val height = 1080
    val src = Img(width, height)
    val dst = Img(width, height)
    val seqtime = standardConfig measure {
      VerticalBoxBlur.blur(src, dst, 0, width, radius)
    }
    println(s"sequential blur time: $seqtime")

    val numTasks = 32
    val partime = standardConfig measure {
      VerticalBoxBlur.parBlur(src, dst, numTasks, radius)
    }
    println(s"fork/join blur time: $partime")
    println(s"speedup: ${seqtime.value / partime.value}")


/** A simple, trivially parallelizable computation. */
object VerticalBoxBlur extends VerticalBoxBlurInterface:

  /** Blurs the columns of the source image `src` into the destination image
   *  `dst`, starting with `from` and ending with `end` (non-inclusive).
   *
   *  Within each column, `blur` traverses the pixels by going from top to
   *  bottom.
   */
  def blur(src: Img, dst: Img, from: Int, end: Int, radius: Int): Unit =
    // TODO implement this method using the `boxBlurKernel` method
    var y = 0
    var x = from

    while(x < end){
      while(y < src.height){
        val newRgba = boxBlurKernel(src, x, y, radius)
        dst.update(x,y,newRgba)
        y = y + 1
      }
      y = 0
      x = x + 1
    }


  /** Blurs the columns of the source image in parallel using `numTasks` tasks.
   *
   *  Parallelization is done by stripping the source image `src` into
   *  `numTasks` separate strips, where each strip is composed of some number of
   *  columns.
   */
  def parBlur(src: Img, dst: Img, numTasks: Int, radius: Int): Unit =
    // TODO implement using the `task` construct and the `blur` method

    // Use Scala ranges to create a list of splitting points
    // (hint: use the by method on ranges).
    // Then use collection combinators on the list of splitting points to create a list
    // of start and end tuples, one for each strip
    // (hint: use the zip and tail methods).
    // Finally, use the task construct to start a parallel task for each strip,
    // and then call join on each task to wait for its completion.

    val x = 0 to src.width by (src.width/Math.min(numTasks, src.width))
    val ranges = x.zip(x.tail)
    val tasks = ranges.map( { case (from, to) => task(blur(src, dst, from, to, radius)) } )
    tasks.foreach(_.join)
    
    
    

