package piwonka.maryl.mapreduce.map

import org.apache.hadoop.fs.{FileContext, FileSystem, Path}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import piwonka.maryl.api.MapReduceContext
import piwonka.maryl.io.{FileBlockIterator, SpillingFileWriter}
import piwonka.maryl.yarn.YarnAppUtils.deserialize

/**
 * EntryPoint for Map-Jobs
 **/
object MapJob extends App {
  val id = sys.env("id") //taskId
  println(s"---MAPPER$id---")
  val mrc = deserialize(new Path(sys.env("MRContext"))).asInstanceOf[MapReduceContext[Any, Any]]
  implicit val fs = FileSystem.get(new YarnConfiguration())
  implicit val fc = FileContext.getFileContext(fs.getUri)
  val job = MapJob(mrc, new FileBlockIterator[(String, Any)](mrc.inputFile, mrc.mapInputParser, Integer.parseInt(id)))
  try {
    job.run()
  }
  catch {
    case e: Exception => e.printStackTrace()
  }
}

/**
 * Logic of a MapJob
 *
 * @param fileIterator An iterator of the specific file block dedicated to the map-job
 **/
case class MapJob[T, U](context: MapReduceContext[T, U], fileIterator: FileBlockIterator[(String, T)])(implicit fs: FileSystem, fc: FileContext) {
  def run() = {
    //create mapper specific subdir in mapOutputDir
    val spillDir = Path.mergePaths(context.mapOutDir, new Path(s"/Mapper${sys.env("id")}"))
    fs.mkdirs(spillDir)
    //create spilling writer for map outputs
    val spillingFileWriter = new SpillingFileWriter[U](spillDir, context.spillBufferSize, context.spillThreshold, context.outputParser, context.reducerCount, context.combineFunction)
    //Higher order functions for streaming data processing
    fileIterator.filter(_.isDefined).flatMap( //Map and flatten results
      elem => {
        val (key, value) = elem.get
        Mapper.map(context.mapFunction, key, value) //run mapper on read data
      }).foreach(spillingFileWriter.write(_)) //write out results
    //Write out last Values
    spillingFileWriter.flush()
    println("---MAPPER FINISHED---")
  }
}
