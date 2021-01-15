package piwonka.maryl.yarn

import org.apache.hadoop.fs.{BlockLocation, FileContext, FileSystem, Path}
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.{AMRMClientAsync, NMClientAsync}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.slf4j.LoggerFactory
import piwonka.maryl.mapreduce.MRJobType.MRJobType
import piwonka.maryl.mapreduce.{DistributedMapReduceOperation, MRJobType}
import piwonka.maryl.yarn.YarnAppUtils._
import piwonka.maryl.yarn.callback.{NMCallbackHandler, RMCallbackHandler}

import scala.jdk.CollectionConverters.ListHasAsScala

case class ApplicationMaster(dmro:DistributedMapReduceOperation)(implicit fs:FileSystem,fc:FileContext){

  private val logger = LoggerFactory.getLogger(classOf[ApplicationMaster])
  private implicit val config:YarnConfiguration = new YarnConfiguration()
  private val nmClient: NMClientAsync = createNMClient(NMCallbackHandler(dmro))
  private val rmClient: AMRMClientAsync[ContainerRequest] = createRMClient(RMCallbackHandler(dmro))
  private val response:RegisterApplicationMasterResponse = rmClient.registerApplicationMaster("", -1, "")//#todo:magic
  private val workerResources:Resource = getWorkerResources(response)
  private var reducerCnt = 0
  private var mapperCnt = 0
  private def getWorkerResources(response: RegisterApplicationMasterResponse): Resource = {
    val maxResources = response.getMaximumResourceCapability
    val mem: Long = math.min(maxResources.getMemorySize, 256)//#todo: from config fix MAGIC
    val vcpu: Int = math.min(maxResources.getVirtualCores, 1)
    Resource.newInstance(mem, vcpu)
  }

  private def createNMClient(nmCallbackHandler: NMCallbackHandler): NMClientAsync = {
    val client = NMClientAsync.createNMClientAsync(nmCallbackHandler)
    client.init(config)
    client.start()
    client
  }

  private def createRMClient(rmCallbackHandler: RMCallbackHandler): AMRMClientAsync[ContainerRequest] = {
    val rmClient = AMRMClientAsync.createAMRMClientAsync[ContainerRequest](1000, rmCallbackHandler)//#todo: Magic
    rmClient.init(config)
    rmClient.start()
    rmClient
  }

  def requestContainers(count:Int)={
    /*val mapRequests:Seq[ContainerRequest] = for(i<-0 until inputFileBlocks.length-1) yield new ContainerRequest(workerResources,inputFileBlocks(i).getHosts,null,Priority.newInstance(1))
    mapRequests.foreach(rmClient.addContainerRequest)
    val mapRequests:Seq[ContainerRequest] = for(i<-0 until inputFileBlocks.length-1) yield new ContainerRequest(workerResources,inputFileBlocks(i).getHosts,null,Priority.newInstance(1))
    */
    val previousAttempts: List[Container] = response.getContainersFromPreviousAttempts.asScala.toList
    val numToRequest: Int = count - previousAttempts.length
    val requests:Seq[ContainerRequest] = (0 until numToRequest).map(_ => new ContainerRequest(workerResources, null, null, Priority.newInstance(1)))//#todo:Magic
    requests.foreach(rmClient.addContainerRequest)
  }

  def setUpWorkerContainerLaunchContext(jobType:MRJobType):ContainerLaunchContext = {
    //SetUp Worker
    val localResource:Map[String,LocalResource] =
      Map(
        "MarylApp.jar" -> setUpLocalResourceFromPath(FileSystem.get(config).makeQualified(new Path(sys.env("HDFSJarPath")))),
      )
    val addEnvVars =
      Map("id"->
          {
            if(jobType==MRJobType.MAP){
              mapperCnt+=1
              s"${mapperCnt-1}"
            }
            else{
              reducerCnt+=1
              s"${reducerCnt-1}"
            }
          },
        "MRContext" -> sys.env("MRContext"),
        "YarnContext" ->sys.env("YarnContext"))
    val workerEnv = buildEnvironment(addEnvVars)
    val workerCommand: List[String] =
      List("$JAVA_HOME/bin/java" +
      s" -Xmx${workerResources.getMemorySize}m " +
      s" $jobType " +
      " 1> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
      " 2> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")

    createContainerContext(workerCommand,localResource,workerEnv)
  }

  def startContainer(container:Container,context:ContainerLaunchContext)={
    nmClient.startContainerAsync(container,context)
  }

  def stop(): Unit = {
    try {
      nmClient.stop()
      rmClient.stop()
    }
    catch{case e:Exception=>e.printStackTrace()}
  }

  def unregisterApplication(appStatus: FinalApplicationStatus, appMessage: String):Unit = {
    try rmClient.unregisterApplicationMaster(appStatus, appMessage, null)
    catch {case e:Exception=>e.printStackTrace()}
    finally stop()
  }

  def freeContainer(containerId: ContainerId): Unit = rmClient.releaseAssignedContainer(containerId)
}







