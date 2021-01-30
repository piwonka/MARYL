package piwonka.maryl.yarn

import com.google.common.io.PatternFilenameFilter
import org.apache.hadoop.fs.{BlockLocation, FileContext, FileSystem, Path}
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.conf.YarnConfiguration
import piwonka.maryl.api.{MapReduceContext, YarnContext}
import piwonka.maryl.io.{FileFinder, FileMergingIterator, TextFileWriter}
import piwonka.maryl.yarn.MRJobType.MRJobType
import piwonka.maryl.yarn.YarnAppUtils.{buildEnvironment, createContainerContext, deserialize, setUpLocalResourceFromPath}

import scala.collection.mutable

object MARYLApplicationMaster {
  def main(args: Array[String]): Unit = {
    implicit val fs: FileSystem = FileSystem.get(new YarnConfiguration())
    implicit val fc: FileContext = FileContext.getFileContext(fs.getUri)
    val yc = deserialize(new Path(sys.env("YarnContext"))).asInstanceOf[YarnContext]
    val mrc = deserialize(new Path(sys.env("MRContext"))).asInstanceOf[MapReduceContext[Any, Any]]
    val status = fs.getFileStatus(mrc.inputFile)
    val blockLocations = fs.getFileBlockLocations(mrc.inputFile, 0, status.getLen)
    println("Blöcke:", blockLocations.length)
    MARYLApplicationMaster(yc, mrc, blockLocations.length, blockLocations).start()
  }
}

case class MARYLApplicationMaster(yc: YarnContext, mrc: MapReduceContext[Any, Any], mapperCount: Int, inputBlockLocations: Array[BlockLocation])(implicit fs: FileSystem, fc: FileContext) extends ApplicationMaster(yc) {
  private val jobs: mutable.HashMap[ContainerId, Worker] = mutable.HashMap()
  private val toRepeat: mutable.Queue[Worker] = mutable.Queue()
  private var finished = 0
  private var mapperCnt = 0
  private var reducerCnt = 0

  def setUpWorkerLaunchContext(jobType: MRJobType): ContainerLaunchContext = {
    //SetUp Worker
    val jobId = {
      if (jobType == MRJobType.MAP) {
        mapperCnt += 1;
        mapperCnt - 1
      } else {
        reducerCnt += 1;
        reducerCnt - 1
      }
    }
    val localResource: Map[String, LocalResource] =
      Map(
        "MarylApp.jar" -> setUpLocalResourceFromPath(FileSystem.get(config).makeQualified(new Path(sys.env("HDFSJarPath")))),
      )
    val addEnvVars =
      Map("id" -> s"$jobId",
        "MRContext" -> sys.env("MRContext"))
    val workerEnv = buildEnvironment(addEnvVars)
    val workerCommand: List[String] =
      List("$JAVA_HOME/bin/java" +
        s" -Xmx${workerResources.getMemorySize}m " +
        s" $jobType " +
        " 1> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
        " 2> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")
    createContainerContext(workerCommand, localResource, workerEnv)
  }

  def start(): Unit = {
    println(s"Requesting $mapperCount mappers and ${mrc.reducerCount} reducers...")
    val mapContainerRequests: Seq[ContainerRequest] = for (i <- 0 until mapperCount) yield new ContainerRequest(workerResources, inputBlockLocations(i).getHosts, null, Priority.newInstance(0))
    mapContainerRequests.foreach(requestContainer)
    val reduceContainerRequests: Seq[ContainerRequest] = (0 until mrc.reducerCount).map(_ => new ContainerRequest(workerResources, null, null, Priority.newInstance(1)))
    reduceContainerRequests.foreach(requestContainer)
  }

  override def handleContainerAllocation(newContainer: Container): Unit = {
    val id = newContainer.getId
    if(!toRepeat.isEmpty){
      val worker=toRepeat.dequeue()
      worker.container=newContainer
      jobs.put(id,worker)
      startContainer(newContainer,worker.context)
    }
    else if (jobs.count(_._2.jobType == MRJobType.MAP) + finished < mapperCount) {
      val mapperLaunchContext = setUpWorkerLaunchContext(MRJobType.MAP)
      jobs.put(id, Worker(mapperLaunchContext,MRJobType.MAP))
      jobs(id).container = newContainer
      startContainer(newContainer, mapperLaunchContext)
    } else {
      val reducerLaunchContext = setUpWorkerLaunchContext(MRJobType.REDUCE)
      jobs.put(id, Worker(reducerLaunchContext,MRJobType.REDUCE))
      jobs(id).container=newContainer
      if (finished >= mapperCount) {
        println("Starting Reducer, that was allocated after Map finished")
        startReducer(newContainer)
      }
    }
  }


  override def handleContainerCompletion(containerStatus: ContainerStatus): Unit = {
    val id = containerStatus.getContainerId
    finished += 1
    println(s"Finish:$finished")
    jobs.remove(id)
    freeContainer(id)
    if (finished == mapperCount) {
      startAllocatedReducers()
    }
    else if (finished == mapperCount + mrc.reducerCount) {
      finalizeResults()
      //deleteTempFiles()
      unregisterApplication(FinalApplicationStatus.SUCCEEDED, "All Jobs completed.")
    }
  }

  private def finalizeResults(): Unit = { //REDUCERS CANT WRITE IN SAME OUTPUT FILE BECAUSE OF BLOCK REPLICATION AND FILE LEASES...
    val reducerResults = FileFinder.find(mrc.copyTargetDir, new PatternFilenameFilter(".+_RESULT.txt"))
    reducerResults.map(_.getName).foreach(println)
    val mergedResults = FileMergingIterator(mrc.reduceInputParser, mrc.comparer, reducerResults)
    val writer = TextFileWriter(mrc.outputFile, mrc.outputParser)
    mergedResults.map(_.get).foreach(writer.write)
    writer.close()
  }

  private def startReducer(container: Container): Unit = {
    val id = container.getId
    val worker = jobs(id)
    startContainer(worker.container, worker.context)
  }

  private def startAllocatedReducers(): Unit = {
    jobs.filter(_._2.jobType == MRJobType.REDUCE).map(_._2.container).foreach(startReducer)
  }

  override def handleContainerError(containerId: ContainerId, error: Throwable): Unit = {
    val failedWorker = jobs(containerId)
    val jobId = Integer.parseInt(failedWorker.context.getEnvironment.get("id"))
    if (failedWorker.attempts < 3) {
      //free failed Container
      freeContainer(failedWorker.container.getId)
      //delete Worker Progress
      cleanUpFailedWorkerAttempt(failedWorker)
      //get Nodes for DataLocality
      val workerNodes = if (failedWorker.jobType == MRJobType.MAP) {
        inputBlockLocations(jobId).getHosts
      } else null
      //make it so that stop is repeated at next container allocation
      failedWorker.attempts+=1
      toRepeat.enqueue(failedWorker)
      jobs.remove(containerId)
      //Request new container
      requestContainer(new ContainerRequest(workerResources, workerNodes, null, Priority.newInstance(0)))
    }
    else unregisterApplication(FinalApplicationStatus.FAILED,s"${failedWorker.jobType.split(".").last} with id ${jobId} took more than 3 attempts to complete")
  }

  private def cleanUpFailedWorkerAttempt(worker: Worker): Unit = {
    val id = worker.context.getEnvironment.get("id")
    val workerDir = if (worker.jobType == MRJobType.MAP) s"/Mapper${id}" else s"/Reducer${id}"
    fs.delete(Path.mergePaths(mrc.mapOutDir, new Path(workerDir)), true)
  }

}
