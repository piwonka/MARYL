package piwonka.maryl.yarn

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.{AMRMClientAsync, NMClientAsync}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import piwonka.maryl.api.YarnContext
import piwonka.maryl.yarn.callback.{NMCallbackHandler, RMCallbackHandler}

/**
 * Base class for ApplicationMaster
 */
abstract class ApplicationMaster(yarnContext: YarnContext)(implicit fs:FileSystem) {


  protected implicit val config: YarnConfiguration = new YarnConfiguration()
  protected val nmClient: NMClientAsync = createNMClient(NMCallbackHandler(this))
  protected val rmClient: AMRMClientAsync[ContainerRequest] = createRMClient(RMCallbackHandler(this))
  protected val response: RegisterApplicationMasterResponse = rmClient.registerApplicationMaster("", -1, "") //#todo:magic
  protected val workerResources: Resource = getWorkerResources(response)

  /**
   * Calculates the resource requirements of Containers based on the user-specified resources and the available resources per container
   * @return A Resource object wrapping the used RAM and VCPU amount
   * @note returns the maximum resources available when the requested resources exceed the maximum
   */
  private def getWorkerResources(response: RegisterApplicationMasterResponse): Resource = {
    val maxResources = response.getMaximumResourceCapability
    val mem: Long = math.min(maxResources.getMemorySize, yarnContext.workerMemory)
    val vcpu: Int = math.min(maxResources.getVirtualCores, yarnContext.workerCores)
    Resource.newInstance(mem, vcpu)
  }

  /**
   * Creates a NodeManager client
   * @param nmCallbackHandler The handler for NodeManager callbacks
   * @return A NodeManager client object
   */
  private def createNMClient(nmCallbackHandler: NMCallbackHandler): NMClientAsync = {
    val client = NMClientAsync.createNMClientAsync(nmCallbackHandler)
    client.init(config)
    client.start()
    client
  }
  /**
   * Creates a ResourceManager client
   * @param rmCallbackHandler The handler for ResourceManager callbacks
   * @return A ResourceManager client object
   */
  private def createRMClient(rmCallbackHandler: RMCallbackHandler): AMRMClientAsync[ContainerRequest] = {
    val rmClient = AMRMClientAsync.createAMRMClientAsync[ContainerRequest](1000, rmCallbackHandler)
    rmClient.init(config)
    rmClient.start()
    rmClient
  }

  /**
   * Requests a container with the specified resources from the ResourceManager.
   * The ResourceManager will allocate an adequate container as soon as it becomes available.
   * @param containerRequest An object wrapping the resource requirements and some other parameters
   */
  protected def requestContainer(containerRequest: ContainerRequest) = rmClient.addContainerRequest(containerRequest)

  /**
   * Starts a subtask on a previously allocated container
   * @param container A container that was previously allocated
   * @param context A context Object wrapping environment, launch command and local resources of the started subtask
   */
  protected def startContainer(container: Container, context: ContainerLaunchContext) = {
    nmClient.startContainerAsync(container, context)
  }

  /**
   * Stops connection to ResourceManager and NodeManagers
   */
  private def stop(): Unit = {
    try {
      nmClient.stop()
      rmClient.stop()
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
   *Unregisters ApplicationMaster with ResourceManager.
   * @param appStatus The state the App finished with. Possible States include: SUCCEEDED,FAILED and KILLED
   */
  protected def unregisterApplication(appStatus: FinalApplicationStatus, appMessage: String): Unit = {
    try rmClient.unregisterApplicationMaster(appStatus, appMessage, null)
    catch {
      case e: Exception => e.printStackTrace()
    }
    finally stop()
  }

  /**
  *Deletes temporary directory of the yarn application.
  */
  protected def deleteTempFiles():Unit={
    fs.delete(yarnContext.tempPath,true)
  }

  /**
   * Frees allocated container for further usage in the cluster
   */
  protected def freeContainer(containerId: ContainerId): Unit = rmClient.releaseAssignedContainer(containerId)

  def handleContainerAllocation(allocatedContainers: Container): Unit

  def handleContainerCompletion(containerStatus: ContainerStatus): Unit

  def handleContainerError(containerId: ContainerId, error: Throwable): Unit

  def getProgress(): Float
}