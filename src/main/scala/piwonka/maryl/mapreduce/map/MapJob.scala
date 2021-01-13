package piwonka.maryl.mapreduce.map

import org.apache.hadoop.fs.{FileContext, FileSystem, Path}
import piwonka.maryl.api.{MapReduceContext, YarnContext}
import piwonka.maryl.io.{FileIterator, SpillingFileWriter}
import piwonka.maryl.yarn.YarnAppUtils.deserialize

object MapJob extends App{
  val yc = deserialize(new Path(sys.env("YarnContext"))).asInstanceOf[YarnContext]
  val mrc = deserialize(new Path(sys.env("MRContext"))).asInstanceOf[MapReduceContext[Any,Any]]
  val job = MapJob(mrc,new FileIterator[(String, Any)](mrc.inputFile,mrc.inputParser)(yc.fc))(yc.fs,yc.fc)
  job.run()
}
case class MapJob[T, U](context: MapReduceContext[T, U], fileIterator: FileIterator[(String, T)])(implicit fs:FileSystem,fc:FileContext){
  def run() = {

    val spillDir = Path.mergePaths(context.mapOutDir,new Path(s"Mapper${sys.env("id")}"))
    val spillingFileWriter = new SpillingFileWriter[U](spillDir, context.spillBufferSize, context.spillThreshold, context.outputParser, context.reducerCount, context.combineFunction)

    //Higher Order Functions instead of Recursion
    fileIterator.flatMap( //Map and flatten results
      elem => {
        val (key, value) = elem.get
        Mapper.map(context.mapFunction, key, value)
      }).foreach(spillingFileWriter.write(_)) //write out results
    //Write out last Values
    spillingFileWriter.flush()
  }
}
