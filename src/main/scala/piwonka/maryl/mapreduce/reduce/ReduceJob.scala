package piwonka.maryl.mapreduce.reduce

import com.google.common.io.PatternFilenameFilter
import org.apache.hadoop.fs.{FileContext, FileSystem, Path}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import piwonka.maryl.api.MapReduceContext
import piwonka.maryl.io._
import piwonka.maryl.yarn.YarnAppUtils.deserialize

/**
 * Entrypoint for Reduce-Jobs
 */
object ReduceJob extends App{
  val id = sys.env("id").toInt
  println(s"---REDUCER$id---")
  val mrc = deserialize(new Path(sys.env("MRContext"))).asInstanceOf[MapReduceContext[Any,Any]]
  implicit val fs = FileSystem.get(new YarnConfiguration())
  implicit val fc = FileContext.getFileContext(fs.getUri)
  val job = ReduceJob(id,mrc)
  job.run()
}

/**
 * Logic of a Reduce-Job
 */
case class ReduceJob[U](id:Int,context: MapReduceContext[_, U])(implicit fs:FileSystem,fc:FileContext){
  def run(): Unit = {

    //Copy
    println(s"Reducer[$id]: starting copy phase...")
    val copyDir = Path.mergePaths(context.copyTargetDir,new Path(s"/Reducer$id")) //create reducer specific
    fs.mkdirs(copyDir)
    val spillingFileWriter = new SpillingFileWriter[U](copyDir, context.copyBufferSize, context.copyBufferSpillThreshold, context.outputParser, 1)
    val mapOutputFiles = FileFinder.find(context.mapOutDir, new PatternFilenameFilter(s".+Partition${id}\\.txt")) //get all corresponding mapOutputs
    val bulkIterator = new FileMergingIterator[(String, U)](context.reduceInputParser, context.comparer, mapOutputFiles)
    bulkIterator.map(_.get).foreach(spillingFileWriter.write(_)) //copy and spill to copyDir
    spillingFileWriter.flush()
    //Merge
    println(s"Reducer[$id]: starting merge phase...")
    val persistentFileMerger = PersistentFileMerger(id, context.mergeFactor, copyDir, context.outputParser, context.reduceInputParser, context.comparer)
    val reducerInput = persistentFileMerger.merge()
    println(s"Partial shuffle complete - Reducer[$id]")

    //Reduce
    println(s"Reducer[$id]: starting reduce phase...")
    val writer = TextFileWriter(Path.mergePaths(copyDir,new Path(s"/Reducer${id}_RESULT.txt")), context.outputParser)
    while(reducerInput.hasNext){
      val input = reducerInput.groupBy(_.get._1)(_.get._2) //group values of same key
      val result = Reducer.reduce(context.reduceFunction,input._1,input._2) //reduce
      writer.write(result)
    }
    writer.close()
    println(s"Reducer [$id] finished.")
  }
}
