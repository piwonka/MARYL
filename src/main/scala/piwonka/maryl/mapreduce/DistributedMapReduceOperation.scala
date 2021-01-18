package piwonka.maryl.mapreduce

import com.google.common.io.PatternFilenameFilter
import org.apache.hadoop.fs.{BlockLocation, FileContext, FileSystem, Path}
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records.{Container, ContainerId, ContainerLaunchContext, ContainerStatus, FinalApplicationStatus, LocalResource, Priority}
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.conf.YarnConfiguration
import piwonka.maryl.api.{MapReduceContext, YarnContext}
import piwonka.maryl.io.{FileFinder, FileMergingIterator, TextFileWriter}
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

case class DistributedMapReduceOperation(yc: YarnContext, mrc: MapReduceContext[Any, Any], mapperCount: Int, inputBlockLocations: Array[BlockLocation])(implicit fs: FileSystem, fc: FileContext) extends ApplicationMaster(yc){
  private val jobs: mutable.HashMap[ContainerId, (MRJobType, Container)] = mutable.HashMap()
  private var finished = 0
  private var mapperCnt=0
  private var reducerCnt=0

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

  def start(): Unit = {
    println(s"Requesting $mapperCount mappers and ${mrc.reducerCount} reducers...")
    val mapContainerRequests: Seq[ContainerRequest] = for (i <- 0 until mapperCount) yield new ContainerRequest(workerResources, inputBlockLocations(i).getHosts, null, Priority.newInstance(0))
    mapContainerRequests.foreach(requestContainer)
    val reduceContainerRequests: Seq[ContainerRequest] = (0 until mrc.reducerCount).map(_ => new ContainerRequest(workerResources, null, null, Priority.newInstance(1)))
    reduceContainerRequests.foreach(requestContainer)
  }

  override def handleContainerAllocation(newContainer: Container): Unit = {
    if (jobs.count(_._2._1 == MRJobType.MAP)+finished < mapperCount) {
      println("Container Host", newContainer.getNodeHttpAddress, newContainer.getNodeId)
      inputBlockLocations.foreach(_.getHosts.foreach(println))
      jobs.put(newContainer.getId, (MRJobType.MAP, newContainer))
      val mapperLaunchContext = setUpWorkerLaunchContext(MRJobType.MAP)
      startContainer(newContainer, mapperLaunchContext)
    } else{
      jobs.put(newContainer.getId, (MRJobType.REDUCE, newContainer))
      if(finished>=mapperCount){
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
    if (finished == mapperCount){
      startAllocatedReducers()
    }
    else if (finished == mapperCount + mrc.reducerCount) {
      finalizeResults()
      //deleteTempFiles()
      unregisterApplication(FinalApplicationStatus.SUCCEEDED, "All Jobs completed.")
    }
  }

  private def finalizeResults(): Unit ={//REDUCERS CANT WRITE IN SAME OUTPUT FILE BECAUSE OF BLOCK REPLICATION AND FILE LEASES...
    val reducerResults = FileFinder.find(mrc.copyDir,new PatternFilenameFilter(".+_RESULT.txt"))
    reducerResults.map(_.getName).foreach(println)
    val mergedResults = FileMergingIterator(mrc.reduceInputParser,mrc.pairComparer,reducerResults)
    val writer = TextFileWriter(mrc.outputFile,mrc.outputParser)
    mergedResults.map(_.get).foreach(writer.write)
    writer.close()
  }

  private def startReducer(container:Container):Unit={
    val context = setUpWorkerLaunchContext(MRJobType.REDUCE)
    startContainer(container, context)
  }

  private def startAllocatedReducers():Unit = {
    //println("Starting AllocatedReducers")
    jobs.filter(_._2._1 == MRJobType.REDUCE).map(_._2._2).foreach(startReducer)
  }

  override def handleContainerError(containerId: ContainerId, error: Throwable): Unit = println(s"ERROR: ${error.getMessage}")

}
