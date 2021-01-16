package piwonka.maryl.yarn

import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.{AMRMClientAsync, NMClientAsync}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import piwonka.maryl.api.YarnContext
import piwonka.maryl.yarn.callback.{NMCallbackHandler, RMCallbackHandler}


abstract class ApplicationMaster(yarnContext: YarnContext) {
  protected implicit val config: YarnConfiguration = new YarnConfiguration()
  protected val nmClient: NMClientAsync = createNMClient(NMCallbackHandler(this))
  protected val rmClient: AMRMClientAsync[ContainerRequest] = createRMClient(RMCallbackHandler(this))
  protected val response: RegisterApplicationMasterResponse = rmClient.registerApplicationMaster("", -1, "") //#todo:magic
  protected val workerResources: Resource = getWorkerResources(response)

  private def getWorkerResources(response: RegisterApplicationMasterResponse): Resource = {
    val maxResources = response.getMaximumResourceCapability
    val mem: Long = math.min(maxResources.getMemorySize, yarnContext.workerMemory)
    val vcpu: Int = math.min(maxResources.getVirtualCores, yarnContext.workerCores)
    Resource.newInstance(mem, vcpu)
  }

  private def createNMClient(nmCallbackHandler: NMCallbackHandler): NMClientAsync = {
    val client = NMClientAsync.createNMClientAsync(nmCallbackHandler)
    client.init(config)
    client.start()
    client
  }

  private def createRMClient(rmCallbackHandler: RMCallbackHandler): AMRMClientAsync[ContainerRequest] = {
    val rmClient = AMRMClientAsync.createAMRMClientAsync[ContainerRequest](1000, rmCallbackHandler)
    rmClient.init(config)
    rmClient.start()
    rmClient
  }

  def requestContainer(containerRequest: ContainerRequest) = rmClient.addContainerRequest(containerRequest)

  def startContainer(container: Container, context: ContainerLaunchContext) = {
    nmClient.startContainerAsync(container, context)
  }

  def stop(): Unit = {
    try {
      nmClient.stop()
      rmClient.stop()
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def unregisterApplication(appStatus: FinalApplicationStatus, appMessage: String): Unit = {
    try rmClient.unregisterApplicationMaster(appStatus, appMessage, null)
    catch {
      case e: Exception => e.printStackTrace()
    }
    finally stop()
  }

  def freeContainer(containerId: ContainerId): Unit = rmClient.releaseAssignedContainer(containerId)

  def handleContainerAllocation(allocatedContainers: Container): Unit

  def handleContainerCompletion(containerStatus: ContainerStatus): Unit

  def handleContainerError(containerId: ContainerId, error: Throwable): Unit
}







