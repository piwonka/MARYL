package piwonka.maryl.mapreduce

import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.records.{Container, ContainerId, FinalApplicationStatus}
import org.slf4j.LoggerFactory
import piwonka.maryl.api.{MapReduceContext, YarnContext}
import piwonka.maryl.mapreduce.MRJobType.MRJobType
import piwonka.maryl.yarn.ApplicationMaster
import piwonka.maryl.yarn.YarnAppUtils.deserialize

import scala.collection.mutable

object DistributedMapReduceOperation {
  def main(args: Array[String]): Unit = {
    val yc = deserialize(new Path(sys.env.get("YarnContext").get)).asInstanceOf[YarnContext]
    val mrc =deserialize(new Path(sys.env.get("MRContext").get)).asInstanceOf[MapReduceContext[Any, Any]]
    DistributedMapReduceOperation(yc,mrc).start()
  }
}

case class DistributedMapReduceOperation(yc:YarnContext,mrc:MapReduceContext[Any,Any]) {
  private val logger = LoggerFactory.getLogger(classOf[DistributedMapReduceOperation])
  private val applicationMaster = ApplicationMaster(this)(yc.fs,yc.fc)
  private val jobs: mutable.HashMap[ContainerId,(MRJobType,Container)] = mutable.HashMap()
  private var finished = 0

  def receiveContainers(containers: mutable.Buffer[Container]): Unit = {
    containers.foreach(c =>
      if(jobs.count(_._2._1==MRJobType.MAP)<mrc.mapperCount){
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
    if(finished==mrc.mapperCount){
      startReducers()
    }
    else if(finished==mrc.mapperCount+mrc.reducerCount){
      applicationMaster.unregisterApplication(FinalApplicationStatus.SUCCEEDED,"All Jobs completed.")
    }
  }

  def start(): Unit = {
    val containerCnt = mrc.mapperCount + mrc.reducerCount
    applicationMaster.requestContainers(containerCnt)
  }

}