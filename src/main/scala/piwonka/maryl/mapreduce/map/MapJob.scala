package piwonka.maryl.mapreduce.map

import org.apache.hadoop.fs.{FileContext, FileSystem, Path}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import piwonka.maryl.api.{MapReduceContext, YarnContext}
import piwonka.maryl.io.{FileBlockIterator, FileIterator, SpillingFileWriter}
import piwonka.maryl.yarn.YarnAppUtils.deserialize

object MapJob extends App{
  val id = sys.env("id")
  println(s"---MAPPER$id---")
  val mrc = deserialize(new Path(sys.env("MRContext"))).asInstanceOf[MapReduceContext[Any,Any]]
  implicit val fs = FileSystem.get(new YarnConfiguration())
  implicit val fc = FileContext.getFileContext(fs.getUri)
  val job = MapJob(mrc,new FileBlockIterator[(String, Any)](mrc.inputFile,mrc.mapInputParser,Integer.parseInt(id)))
  job.run()
}
case class MapJob[T, U](context: MapReduceContext[T, U], fileIterator: FileBlockIterator[(String, T)])(implicit fs:FileSystem,fc:FileContext){
  def run() = {

    val spillDir = Path.mergePaths(context.mapOutDir,new Path(s"/Mapper${sys.env("id")}"))
    fs.mkdirs(spillDir)
    val spillingFileWriter = new SpillingFileWriter[U](spillDir, context.spillBufferSize, context.spillThreshold, context.outputParser, context.reducerCount, context.combineFunction)

    //Higher Order Functions instead of Recursion
    fileIterator.filter(_.isDefined).flatMap( //Map and flatten results
      elem => {
        val (key, value) = elem.get
        Mapper.map(context.mapFunction, key, value)
      }).foreach(spillingFileWriter.write(_)) //write out results
    //Write out last Values
    spillingFileWriter.flush()
    println("---MAPPER FINISHED---")
  }
}
