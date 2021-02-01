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

import scala.Console.err
import scala.collection.mutable

/**
 * Entrypoint for ApplicationMaster
 */
object MARYLApplicationMaster {
  def main(args: Array[String]): Unit = {
    implicit val fs: FileSystem = FileSystem.get(new YarnConfiguration())
    implicit val fc: FileContext = FileContext.getFileContext(fs.getUri)
    //deserialize context
    val yc = deserialize(new Path(sys.env("YarnContext"))).asInstanceOf[YarnContext]
    val mrc = deserialize(new Path(sys.env("MRContext"))).asInstanceOf[MapReduceContext[Any, Any]]
    val status = fs.getFileStatus(mrc.inputFile)
    //Get BlockLocations of input File
    val blockLocations = fs.getFileBlockLocations(mrc.inputFile, 0, status.getLen)
    println("block count:", blockLocations.length)
    MARYLApplicationMaster(yc, mrc, blockLocations.length, blockLocations).start()
  }
}

/**
 *Contains the logic of an MapReduce-ApplicationMaster
 * @note ApplicationMaster logic is largely callback driven and is cluttered as suc
 * @param yc The YarnContext specified by the User
 * @param mrc The MapReduceContext specified by the User
 * @param mapperCount The number of mappers created (== inputBlockLocations.length)
 * @param inputBlockLocations The block locations of the input file
 */
case class MARYLApplicationMaster(yc: YarnContext, mrc: MapReduceContext[Any, Any], mapperCount: Int, inputBlockLocations: Array[BlockLocation])(implicit fs: FileSystem, fc: FileContext) extends ApplicationMaster(yc) {
  private val jobs: mutable.HashMap[ContainerId, Worker] = mutable.HashMap()  //Keeps all information on containers and running jobs
  private val toRepeat: mutable.Queue[Worker] = mutable.Queue() //Schedules failed tasks for repeat
  private var finished = 0 //counts finished subtasks
  private var mapperId = 0 //current Mapper-Id to be given to the next Mapper
  private var reducerId = 0 //current Reducer-Id to be given to the next Reducer

  /**
   * Creates a ContainerLaunchContext for a subtask, depending on the specified jobType
   * @param jobType The subtask to be prepared (Mapper/Reducer)
   */
  def setUpWorkerLaunchContext(jobType: MRJobType): ContainerLaunchContext = {
    //Calculate Id for worker
    val jobId = {
      if (jobType == MRJobType.MAP) {
        mapperId += 1
        mapperId - 1
      } else {
        reducerId += 1
        reducerId - 1
      }
    }
    //Set Application jarfile as LocalResource
    val localResource: Map[String, LocalResource] =
      Map(
        "MarylApp.jar" -> setUpLocalResourceFromPath(FileSystem.get(config).makeQualified(new Path(sys.env("HDFSJarPath")))),//jar location was passed to AM through an environmental variable
      )
    //Add job id and MapReduceContext to environmental variables
    val addEnvVars =
      Map("id" -> s"$jobId",
        "MRContext" -> sys.env("MRContext"))
    val workerEnv = buildEnvironment(addEnvVars)
    //set up worker command on the basis of the jobType
    val workerCommand: List[String] =
      List("$JAVA_HOME/bin/java" +
        s" -Xmx${workerResources.getMemorySize}m " +
        s" $jobType " +
        " 1> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
        " 2> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")

    createContainerContext(workerCommand, localResource, workerEnv)
  }

  /**
   * Requests all necessary Containers for the ApplicationMaster
   * @note Map-Containers are requested at a higher priority to ensure that they will be allocated first.
   */
  def start(): Unit = {
    println(s"Requesting $mapperCount mappers and ${mrc.reducerCount} reducers...")
    val mapContainerRequests: Seq[ContainerRequest] = for (i <- 0 until mapperCount) yield new ContainerRequest(workerResources, inputBlockLocations(i).getHosts, null, Priority.newInstance(0))
    mapContainerRequests.foreach(requestContainer)
    val reduceContainerRequests: Seq[ContainerRequest] = (0 until mrc.reducerCount).map(_ => new ContainerRequest(workerResources, null, null, Priority.newInstance(1)))
    reduceContainerRequests.foreach(requestContainer)
  }

  /**
   * Starts or keeps allocated containers
   * @param newContainer The newly allocated Container
   */
  override def handleContainerAllocation(newContainer: Container): Unit = {
    val id = newContainer.getId
    println(s"Container[$id] allocated.")
    if(toRepeat.nonEmpty) { //check if there are tasks that need to be repeated
      val worker = toRepeat.dequeue()
      worker.container = newContainer //set container of task
      jobs.put(id, worker) //read task to running jobs
      startContainer(newContainer, worker.context) //start task
    }
    else if (jobs.count(_._2.jobType == MRJobType.MAP) + finished < mapperCount) { //check if there is a container for each map-task
      val mapperLaunchContext = setUpWorkerLaunchContext(MRJobType.MAP) //prepare map task
      jobs.put(id, Worker(mapperLaunchContext,MRJobType.MAP))
      jobs(id).container = newContainer //add container to worker object
      startContainer(newContainer, mapperLaunchContext) //launch map task
    } else { //if there is a container for each map-task create reducers
      val reducerLaunchContext = setUpWorkerLaunchContext(MRJobType.REDUCE) //prepare reduce task
      jobs.put(id, Worker(reducerLaunchContext,MRJobType.REDUCE))
      jobs(id).container=newContainer//add container to worker object
      if (finished >= mapperCount) { //dont start task unless all mappers have already finished
        println("Starting Reducer, that was allocated after Map finished")
        startContainer(newContainer,reducerLaunchContext)
      }
    }
  }

  /**
   * Handles the completion of a subtask
   * @param containerStatus the completion status of the Container
   * @note The Class ContainerStatus implies that there could be failed containers with a FAILED state being counted as finished here
   *       The underlying ContainerState and ContainerSubState only supply the States RUNNING,PAUSED,COMPLETE,NEW and SCHEDULED
   */
  override def handleContainerCompletion(containerStatus: ContainerStatus): Unit = {
    val id = containerStatus.getContainerId
    finished += 1 //count subtask as finished
    println(s"Container[$id] finished")
    jobs.remove(id)
    freeContainer(id)
    if (finished == mapperCount) {  //start reducers if container was the last map task to complete
      startAllocatedReducers()
    }
    else if (finished == mapperCount + mrc.reducerCount) {  //end application if all subtasks have been completed
      finalizeResults() //collect results in output file
      unregisterApplication(FinalApplicationStatus.SUCCEEDED, "All Jobs completed.")
    }
  }

  /**
   * Collect reducer results and merge into single output file
   * @note reduce tasks cant write to the same output file. Further Reading: Block replication and file leases
   */
  private def finalizeResults(): Unit = {
    val reducerResults = FileFinder.find(mrc.copyTargetDir, new PatternFilenameFilter(".+_RESULT.txt"))
    reducerResults.map(_.getName).foreach(println)
    val mergedResults = FileMergingIterator(mrc.reduceInputParser, mrc.comparer, reducerResults)
    val writer = TextFileWriter(mrc.outputFile, mrc.outputParser)
    mergedResults.map(_.get).foreach(writer.write)
    writer.close()
  }

  /**
   * Starts all Containers that belong to reduce-tasks and were allocated before all map-tasks where finished
   */
  private def startAllocatedReducers(): Unit = {
    jobs.filter(_._2.jobType == MRJobType.REDUCE).values.foreach(w=>startContainer(w.container,w.context))
  }

  /**
   * Handles failed Containers by rescheduling them
   * @param containerId the id of the failed container
   * @param error the error that caused the failure
   */
  override def handleContainerError(containerId: ContainerId, error: Throwable): Unit = {
    val failedWorker = jobs(containerId)
    err.println(s"Container[$containerId] failed unexpectedly - ${error.getMessage}")
    err.println(s"Attempting to reschedule Container[$containerId]")
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
    else unregisterApplication(FinalApplicationStatus.FAILED,s"${failedWorker.jobType.split(".").last} with id $jobId took more than 3 attempts to complete")
  }

  /**
   * Finds and deletes all directories created by the worker
   */
  private def cleanUpFailedWorkerAttempt(worker: Worker): Unit = {
    val id = worker.context.getEnvironment.get("id")
    val workerDirName = if (worker.jobType == MRJobType.MAP) s"/Mapper$id" else s"/Reducer$id"
    val workerDirPath = Path.mergePaths((if (worker.jobType == MRJobType.MAP) mrc.mapOutDir else mrc.copyTargetDir), new Path(workerDirName))
    fs.delete(workerDirPath, true)
  }

  /**
   * Calculates progress based on the amount of subtasks that have finished
   */
  override def getProgress(): Float = (100f/(mapperCount*mrc.reducerCount))*finished
}
