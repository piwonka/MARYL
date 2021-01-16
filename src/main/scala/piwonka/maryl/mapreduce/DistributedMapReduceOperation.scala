package piwonka.maryl.mapreduce

import org.apache.hadoop.fs.{BlockLocation, FileContext, FileSystem, Path}
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records.{Container, ContainerId, ContainerLaunchContext, ContainerStatus, FinalApplicationStatus, LocalResource, Priority}
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.slf4j.LoggerFactory
import piwonka.maryl.api.{MapReduceContext, YarnContext}
import piwonka.maryl.mapreduce.MRJobType.MRJobType
import piwonka.maryl.yarn.ApplicationMaster
import piwonka.maryl.yarn.YarnAppUtils.{buildEnvironment, createContainerContext, deserialize, setUpLocalResourceFromPath}
import scala.collection.mutable

object DistributedMapReduceOperation {
  def main(args: Array[String]): Unit = {
    implicit val fs:FileSystem = FileSystem.get(new YarnConfiguration())
    implicit val fc:FileContext = FileContext.getFileContext(fs.getUri)
    val yc = deserialize(new Path(sys.env("YarnContext"))).asInstanceOf[YarnContext]
    val mrc = deserialize(new Path(sys.env("MRContext"))).asInstanceOf[MapReduceContext[Any, Any]]
    val status = fs.getFileStatus(mrc.inputFile)
    val blockLocations = fs.getFileBlockLocations(mrc.inputFile, 0, status.getLen)
    println("Blöcke:", blockLocations.length)
    DistributedMapReduceOperation(yc, mrc, blockLocations.length, blockLocations).start()
  }
}

case class DistributedMapReduceOperation(yc: YarnContext, mrc: MapReduceContext[Any, Any], mapperCount: Int, inputBlockLocations: Array[BlockLocation])(implicit fs: FileSystem, fc: FileContext) extends ApplicationMaster(yc) {
  private val logger = LoggerFactory.getLogger(classOf[DistributedMapReduceOperation])
  private val jobs: mutable.HashMap[ContainerId, (MRJobType, Container)] = mutable.HashMap()
  private val workedBlocks: Array[Boolean] = new Array(inputBlockLocations.length)
  private var finished = 0
  private var mapperCnt, reducerCnt = 0

  def setUpWorkerLaunchContext(jobType: MRJobType): ContainerLaunchContext = {
    //SetUp Worker
    val localResource: Map[String, LocalResource] =
      Map(
        "MarylApp.jar" -> setUpLocalResourceFromPath(FileSystem.get(config).makeQualified(new Path(sys.env("HDFSJarPath")))),
      )
    val addEnvVars =
      Map("id" -> {
        if (jobType == MRJobType.MAP) {
          mapperCnt += 1
          s"${mapperCnt - 1}"
        }
        else {
          reducerCnt += 1
          s"${reducerCnt - 1}"
        }
      },
        "MRContext" -> sys.env("MRContext"),
        "YarnContext" -> sys.env("YarnContext"))
    val workerEnv = buildEnvironment(addEnvVars)
    val workerCommand: List[String] =
      List("$JAVA_HOME/bin/java" +
        s" -Xmx${workerResources.getMemorySize}m " +
        s" $jobType " +
        " 1> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
        " 2> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")
    createContainerContext(workerCommand, localResource, workerEnv)
  }



  private def startReducers():Unit = {
    val reducerContainers = jobs.filter(_._2._1 == MRJobType.REDUCE).map(_._2._2).toList
    for (i <- reducerContainers.indices) {
      val context = setUpWorkerLaunchContext(MRJobType.REDUCE)
      startContainer(reducerContainers(i), context)
    }
  }

  def start(): Unit = {
    println(s"Requesting $mapperCount mappers and ${mrc.reducerCount} reducers...")
    val mapContainerRequests: Seq[ContainerRequest] = for (i <- 0 until mapperCount) yield new ContainerRequest(workerResources, inputBlockLocations(i).getHosts, null, Priority.newInstance(1))
    mapContainerRequests.foreach(requestContainer)
    val reduceContainerRequests: Seq[ContainerRequest] = (0 until mrc.reducerCount).map(_ => new ContainerRequest(workerResources, null, null, Priority.newInstance(1)))
    reduceContainerRequests.foreach(requestContainer)
    println(s"Requesting $mapperCount mappers and ${mrc.reducerCount} reducers...")
  }

  override def handleContainerAllocation(newContainer: Container): Unit = {
    if (jobs.count(_._2._1 == MRJobType.MAP) < mapperCount) {
      println("Container Host", newContainer.getNodeHttpAddress, newContainer.getNodeId)
      inputBlockLocations.foreach(_.getHosts.foreach(println))
      jobs.put(newContainer.getId, (MRJobType.MAP, newContainer))
      val mapperLaunchContext = setUpWorkerLaunchContext(MRJobType.MAP)
      startContainer(newContainer, mapperLaunchContext)
    } else jobs.put(newContainer.getId, (MRJobType.REDUCE, newContainer))
  }


  override def handleContainerCompletion(containerStatus: ContainerStatus): Unit = {
    val id = containerStatus.getContainerId
    finished += 1
    jobs.remove(id)
    freeContainer(id)
    if (finished == mapperCount&&jobs.count(_._2._1==MRJobType.REDUCE)==mrc.reducerCount) {
      startReducers()
    }
    else if (finished == mapperCount + mrc.reducerCount) {
      unregisterApplication(FinalApplicationStatus.SUCCEEDED, "All Jobs completed.")
    }
  }

  override def handleContainerError(containerId: ContainerId, error: Throwable): Unit = ???

  def restartContainer(containerId: ContainerId):Unit = ???

  def findOptimalBlockLocationForContainer(container: Container): Int = ???
}
