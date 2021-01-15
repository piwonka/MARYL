package piwonka.maryl.mapreduce

import org.apache.hadoop.fs.{BlockLocation, FSDataInputStream, FileContext, FileSystem, Path}
import org.apache.hadoop.io.compress.{CodecPool, CompressionCodecFactory}
import org.apache.hadoop.yarn.api.records.{Container, ContainerId, FinalApplicationStatus}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.slf4j.LoggerFactory
import piwonka.maryl.api.{MapReduceContext, YarnContext}
import piwonka.maryl.mapreduce.MRJobType.MRJobType
import piwonka.maryl.yarn.ApplicationMaster
import piwonka.maryl.yarn.YarnAppUtils.deserialize

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object DistributedMapReduceOperation {
  def main(args: Array[String]): Unit = {
    println("DMRO Started")
    println(sys.env("YarnContext"),sys.env("MRContext"))
    println(deserialize(new Path(sys.env("YarnContext"))))
    implicit val fs = FileSystem.get(new YarnConfiguration())
    implicit val fc = FileContext.getFileContext(fs.getUri)
    val yc = deserialize(new Path(sys.env("YarnContext"))).asInstanceOf[YarnContext]
    val mrc =deserialize(new Path(sys.env("MRContext"))).asInstanceOf[MapReduceContext[Any, Any]]
    val status = fs.getFileStatus(mrc.inputFile)
    val blockLocations = fs.getFileBlockLocations(mrc.inputFile,0,status.getLen)
    DistributedMapReduceOperation(yc,mrc,blockLocations.length-1).start()
  }
}

case class DistributedMapReduceOperation(yc:YarnContext,mrc:MapReduceContext[Any,Any],mapperCount:Int)(implicit fileSystem:FileSystem,fileContext:FileContext) {
  private val logger = LoggerFactory.getLogger(classOf[DistributedMapReduceOperation])
  private val applicationMaster = ApplicationMaster(this)
  private val jobs: mutable.HashMap[ContainerId,(MRJobType,Container)] = mutable.HashMap()
  private var finished = 0

  def receiveContainers(containers: mutable.Buffer[Container]): Unit = {
    containers.foreach(c =>
      if(jobs.count(_._2._1==MRJobType.MAP)<mapperCount){
        jobs.put(c.getId,(MRJobType.MAP,c))
        val context = applicationMaster.setUpWorkerContainerLaunchContext(MRJobType.MAP)
        applicationMaster.startContainer(c,context)
      }else jobs.put(c.getId,(MRJobType.REDUCE,c)))
    logger.debug(s"${containers.length} containers allocated")
  }

  private def startReducers()={
    val reducerContainers=jobs.filter(_._2._1==MRJobType.REDUCE).map(_._2._2).toList
    for(i <- 0 until reducerContainers.length){
      val context = applicationMaster.setUpWorkerContainerLaunchContext(MRJobType.REDUCE)
      applicationMaster.startContainer(reducerContainers(i),context)
    }
  }

  def markTaskCompleted(containerId: ContainerId): Unit = {
    finished += 1
    jobs.remove(containerId)
    applicationMaster.freeContainer(containerId)
    if(finished==mapperCount){
      startReducers()
    }
    else if(finished==mapperCount+mrc.reducerCount){
      applicationMaster.unregisterApplication(FinalApplicationStatus.SUCCEEDED,"All Jobs completed.")
    }
  }
  def start(): Unit = {
    applicationMaster.requestContainers(mapperCount+mrc.reducerCount)
  }
}
