package piwonka.maryl.yarn.callback

import java.nio.ByteBuffer
import java.util

import org.apache.hadoop.yarn.api.records.{ContainerId, ContainerStatus, Resource}
import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import org.slf4j.LoggerFactory
import piwonka.maryl.yarn.ApplicationMaster

/**
 * Handler used to receive callbacks from different NodeManagers running subtasks of the application.
 * @param appMaster The ApplicationMaster to be notified of callbacks
 * @note mainly used to trigger a task repeat of a failed task through the application master
 */
case class NMCallbackHandler(appMaster:ApplicationMaster) extends NMClientAsync.AbstractCallbackHandler{
  private val logger = LoggerFactory.getLogger(classOf[NMCallbackHandler])

  override def onContainerStatusReceived(containerId: ContainerId, containerStatus: ContainerStatus): Unit = logger.info("Container status: id={}, status={}.", containerId, containerStatus.asInstanceOf[Any])

  override def onContainerStarted(containerId: ContainerId, map: util.Map[String, ByteBuffer]): Unit = {logger.info("Container started [{}].", containerId)}

  override def onContainerResourceIncreased(containerId: ContainerId, resource: Resource): Unit = logger.info("Container [{}] resources increased  with [{}]. ", containerId, resource.asInstanceOf[Any])

  override def onContainerResourceUpdated(containerId: ContainerId, resource: Resource): Unit = logger.info("Container [{}] resource updated  with [{}]. ", containerId, resource.asInstanceOf[Any])

  override def onGetContainerStatusError(containerId: ContainerId, error: Throwable): Unit = {
    logger.error("Failed to get container [{}]. {}", containerId, error.asInstanceOf[String])
    appMaster.handleContainerError(containerId,error)
  }

  override def onIncreaseContainerResourceError(containerId: ContainerId, error: Throwable): Unit = logger.error("Container [{}] failed to increase resources.{}", containerId, error.asInstanceOf[Any])

  override def onUpdateContainerResourceError(containerId: ContainerId, error: Throwable): Unit = {

    logger.error("Container [{}] failed to update resources.{}", containerId, error.asInstanceOf[String])
    appMaster.handleContainerError(containerId,error)
  }

  override def onStopContainerError(containerId: ContainerId, error: Throwable): Unit =  {logger.error("Failed to stop container [{}].", containerId)
    logger.error("Error stopping container [{}].{}", containerId, error.asInstanceOf[String])
    appMaster.handleContainerError(containerId,error)
  }

  override def onStartContainerError(containerId: ContainerId, error: Throwable): Unit ={
    appMaster.handleContainerError(containerId,error)
    logger.error("Error starting container [{}].{}", containerId, error.asInstanceOf[String])
  }

  override def onContainerStopped(containerId: ContainerId): Unit = logger.info("Container stopped [{}].", containerId)

}
