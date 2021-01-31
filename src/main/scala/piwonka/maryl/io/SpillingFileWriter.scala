package piwonka.maryl.io

import org.apache.hadoop.fs.{FileContext, Path}
import piwonka.maryl.mapreduce.combine.Combiner

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
/**A FileWriter that buffers data and asynchronously partitions and spills it to the HDFS
 * @param spillDir The Directory where the generated spillfiles are written to
 * @param parser A function that transforms output data to the desired output format
 * @param spillBufferSize The size of the buffer
 * @param spillThreshold The threshold that needs to be reached to start a spill
 * @param partitionCnt The number of partitions created for each spill
 * @param combineFunction The function that is fed to the combiner, which is called before every spill
 **/
case class SpillingFileWriter[U](spillDir: Path, spillBufferSize: Int, spillThreshold: Float, parser: ((String, U)) => String, partitionCnt: Int, combineFunction: (U, U) => U = null)(implicit fc: FileContext) {

  case class CircularBuffer[T](bufferSize: Int)(implicit tag: ClassTag[T]) {
    val buffer = new Array[T](bufferSize)
    var start, end: Int = 0
    var length = 0

    def elementCount(): Int = {
      length
    }

    def add(elem: T): Unit = {
      buffer(end) = elem
      end = (end + 1) % (buffer.length)
      length += 1
    }

    def getValuesAndReset(): List[T] = {
      val result = if (end <= start) buffer.slice(start, buffer.length) ++ buffer.slice(0, end) else buffer.slice(start, end)
      start = end
      length = 0
      result.toList
    }
  }
  val buffer = new CircularBuffer[(String, U)](spillBufferSize)
  var spillState: Future[Unit] = null /* Deliberate decision to keep the Future of the current spill as state, to circumvent race-conditions when reading from the buffer. ->leads to only one spill happening at once*/
  var spillCnt = 0 //Just  needed for unique filenames

  /**
   * Writes a pair to buffer
   * @note calls write(key:String,value:U)
   */
  def write(pair: (String, U)): Unit = {
    write(pair._1, pair._2)
  }

  /**
   * Writes a pair to the buffer, checking first if the buffer reached the spillthreshold.
   * Initiates asynchronous spill if threshold is reached, before writing the pair to the buffer.
   */
  def write(key: String, value: U): Unit = {
    //Create Spill if Buffer over Threshhold
    if (spillBufferSize * spillThreshold <= buffer.elementCount()) {
      //If full and spill is not finished wait
      waitUntilCurrentSpillFinished()
      //Spill
      val values = buffer.getValuesAndReset()
      spillState = Future { //initiate asynchronous spill
        spill(values)
      }
    }
    //Write to Buffer
    buffer.add((key, value))
  }


  /**
   *Sort, partition and combine buffered elements before writing them into spill files.
   * Every partition corresponds to their own spillfile
   * @param elements The values of the circular buffer that will be spilled
   */
  private def spill(elements: List[(String, U)]) = {
    val spillFileName = Path.mergePaths(spillDir, new Path(s"/Spill${spillCnt}_Id${Thread.currentThread().getId}_Partition")).toString
    spillCnt += 1
    val sortedElements = elements.sortBy(_._1)
    //Partition and write out pairs
    val writers = new Array[TextFileWriter[(String, U)]](partitionCnt)
    for (pair <- if (combineFunction == null) sortedElements else combine(sortedElements)) { //if a combiner is specified run it on the sorted buffer elements
      val partitionIndex = (pair._1.hashCode & Integer.MAX_VALUE) % partitionCnt
      //create writer if that specific partition wasnt written to before
      writers(partitionIndex) = if (writers(partitionIndex) == null) TextFileWriter[(String, U)](new Path(spillFileName + s"$partitionIndex.txt"), parser) else writers(partitionIndex)
      writers(partitionIndex).write(pair)
    }
    writers.filter(_ != null).foreach(_.close())//close all opened writers.
  }

  /**
   *Groups pairs by key in a map, isolates values and runs the combiner on them, then returns list of (Key,CombinedValue)
   * @param elements The sorted list of pairs that will be combined
   */
  private def combine(elements: List[(String, U)]): List[(String, U)] = {
    val groupedElements:Map[String,List[U]] = elements.groupBy(_._1).transform((_, pairs) => pairs.map(_._2)) //group data by key
    groupedElements.map(e => Combiner.combine(combineFunction, e._1, e._2)).toList //run combiner on grouped data and return
  }

  /**
   *initiates a spill of all values in the buffer, regardless of the reached threshold
   * @note Needs to exist because in the end the buffer needs to force a spill to write out remaining values without reaching the threshold
   */
  def flush() = {
    waitUntilCurrentSpillFinished()
    if (buffer.elementCount() > 0) spill(buffer.getValuesAndReset())
  }
  /**
   * Waits for the current Spill to finish
   * @note wait is blocking.
   * */
  private def waitUntilCurrentSpillFinished(): Unit ={
    while (spillState != null && !spillState.isCompleted) {
      /*#todo:In the final version of this Library the Duration of the wait should not be infinite

      However, it is hard to predict the speed of other clusters. As long as hdfs is properly functioning this should be a non-issue*/
      Await.ready(spillState, Duration.Inf)
    }
  }
}
