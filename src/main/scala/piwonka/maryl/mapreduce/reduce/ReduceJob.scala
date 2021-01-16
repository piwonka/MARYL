package piwonka.maryl.mapreduce.reduce

import com.google.common.io.PatternFilenameFilter
import org.apache.hadoop.fs.{FileContext, FileSystem, Path}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import piwonka.maryl.api.{MapReduceContext, YarnContext}
import piwonka.maryl.io._
import piwonka.maryl.yarn.YarnAppUtils.deserialize

object ReduceJob extends App{
  val yc = deserialize(new Path(sys.env("YarnContext"))).asInstanceOf[YarnContext]
  val mrc = deserialize(new Path(sys.env("MRContext"))).asInstanceOf[MapReduceContext[Any,Any]]
  implicit val fs = FileSystem.get(new YarnConfiguration())
  implicit val fc = FileContext.getFileContext(fs.getUri)
  val job = ReduceJob(sys.env("id").toInt,mrc)
  job.run()
}

case class ReduceJob[U](id:Int,context: MapReduceContext[_, U])(implicit fs:FileSystem,fc:FileContext) extends Thread{
  override def run(): Unit = {
    println("Copy ["+id+"]")
    //Copy
    val copyDir = Path.mergePaths(context.copyDir,new Path(s"/Reducer$id"))
    fs.mkdirs(copyDir)
    val spillingFileWriter = new SpillingFileWriter[U](copyDir, context.copyBufferSize, context.copyBufferSpillThreshold, context.outputParser, 1)
    val mapOutputFiles = FileFinder.find(context.mapOutDir, new PatternFilenameFilter(s".+Partition${id}\\.txt"))
    mapOutputFiles.map(_.getName).foreach(println(_))
    val bulkIterator = new FileMergingIterator[(String, U)](context.reduceInputParser, context.pairComparer, mapOutputFiles)
    bulkIterator.map(_.get).foreach(spillingFileWriter.write(_)) //copy and spill to copyDir
    spillingFileWriter.flush()
    //Merge
    System.out.println("Merge ["+id+"]")
    val persistentFileMerger = PersistentFileMerger(id, context.fileCntPerMerge, copyDir, context.outputParser, context.reduceInputParser, context.pairComparer)
    val reducerInput = persistentFileMerger.merge()
    println("Merge["+id+"] finished")
    //Reduce

    while(reducerInput.hasNext){
      val input = reducerInput.groupBy(_.get._1)(_.get._2)
      val result = Reducer.reduce(context.reduceFunction,input._1,input._2)
      val writer = TextFileWriter(context.outputFile, context.outputParser)
      writer.write(result)
      writer.close()
    }
    println("Reducer [" + id + "] finished")
  }
}
