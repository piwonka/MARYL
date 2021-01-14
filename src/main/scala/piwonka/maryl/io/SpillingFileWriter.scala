package piwonka.maryl.io

import org.apache.hadoop.fs.{FileContext, Path}
import piwonka.maryl.mapreduce.combine.Combiner

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag

case class SpillingFileWriter[U](spillDir:Path, spillBufferSize:Int, spillThreshold:Float, parser:((String,U))=>String, partitionCnt:Int, combiner:(U,U)=>U=null)(implicit fc:FileContext){
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
      length+=1
    }

    def getValuesAndReset(): List[T] = {
      val result = if (end <= start) buffer.slice(start, buffer.length) ++ buffer.slice(0, end) else buffer.slice(start, end)
      start = end
      length=0
      result.toList
    }
  }

  var spillCnt = 0 //Just for unique filenames
  val buffer = new CircularBuffer[(String, U)](spillBufferSize)
  var spillState: Future[Unit] = null //Empty future for completed state in while(bandaid fix)

  def write(pair:(String,U)):Unit={
    write(pair._1,pair._2)
  }
  def write(key: String, value: U): Unit = {
    //Create Spill if Buffer over Threshhold
    if (spillBufferSize*spillThreshold <= buffer.elementCount()) {
      //If full and spill is not finished wait
      while (spillState!=null && !spillState.isCompleted) {
        Await.ready(spillState, Duration.Inf)
      }
      //Spill
      val values = buffer.getValuesAndReset()
      spillState = Future {
        spill(values)
      }
    }
    //Write to Buffer
    buffer.add((key, value))
  }

  private def spill(elements: List[(String, U)]) = {
    val spillFileName = Path.mergePaths(spillDir,new Path(s"/Spill${spillCnt}_Id${Thread.currentThread().getId}_Partition")).toString
    spillCnt += 1
    val sortedElements = elements.sortBy(_._1)
    //Partition and write out pairs
    val writers = for(i<-0 until partitionCnt) yield TextFileWriter[(String,U)](new Path(spillFileName+s"$i.txt"),parser)
    for(pair<-if(combiner==null) sortedElements else combine(sortedElements)){//Foreach would be possible, but this is returning Unit and foreach isnt meant for state operations
      val partitionIndex = pair._1.hashCode % partitionCnt
      writers(partitionIndex).write(pair)
    }
    writers.foreach(_.close())
  }

  //Sort every instance of pair(_) by key (._1), group them by key in a map, isolate values and run combiner on them
  private def combine(elements:List[(String,U)]):List[(String,U)]={
    val groupedElements = elements.groupBy(_._1).transform((_,pairs)=>pairs.map(_._2)) //group data by key
    groupedElements.map(e=>Combiner.combine(combiner,e._1,e._2)).toList
  }

  //Needs to exist because in the end the buffer needs to force a spill to write out all values
  def flush() = if(buffer.elementCount()>0) spill(buffer.getValuesAndReset())
}
