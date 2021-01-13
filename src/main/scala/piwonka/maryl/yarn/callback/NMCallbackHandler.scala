package piwonka.maryl.yarn.callback

import java.nio.ByteBuffer
import java.util

import org.apache.hadoop.yarn.api.records.{ContainerId, ContainerStatus, Resource}
import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import org.slf4j.LoggerFactory
import piwonka.maryl.mapreduce.DistributedMapReduceOperation

case class NMCallbackHandler(dmro:DistributedMapReduceOperation) extends NMClientAsync.AbstractCallbackHandler{
  private val logger = LoggerFactory.getLogger(classOf[NMCallbackHandler])

  override def onContainerStatusReceived(containerId: ContainerId, containerStatus: ContainerStatus): Unit = logger.info("Container status: id={}, status={}.", containerId, containerStatus.asInstanceOf[Any])

  override def onContainerStarted(containerId: ContainerId, map: util.Map[String, ByteBuffer]): Unit = {logger.info("Container started [{}].", containerId)}

  override def onContainerResourceIncreased(containerId: ContainerId, resource: Resource): Unit = logger.info("Container [{}] resources increased  with [{}]. ", containerId, resource.asInstanceOf[Any])

  override def onContainerResourceUpdated(containerId: ContainerId, resource: Resource): Unit = logger.info("Container [{}] resource updated  with [{}]. ", containerId, resource.asInstanceOf[Any])

  override def onGetContainerStatusError(containerId: ContainerId, error: Throwable): Unit = logger.error("Failed to get container [{}]. {}", containerId, error.asInstanceOf[Any])

  override def onIncreaseContainerResourceError(containerId: ContainerId, error: Throwable): Unit = logger.error("Container [{}] failed to increase resources.{}", containerId, error.asInstanceOf[Any])

  override def onUpdateContainerResourceError(containerId: ContainerId, error: Throwable): Unit = logger.error("Container [{}] failed to update resources.{}", containerId, error.asInstanceOf[Any])

  override def onStopContainerError(containerId: ContainerId, t: Throwable): Unit =  logger.error("Failed to stop container [{}].", containerId)

  override def onStartContainerError(containerId: ContainerId, error: Throwable): Unit = logger.error("Error starting container [{}].{}", containerId, error.asInstanceOf[Any])

  override def onContainerStopped(containerId: ContainerId): Unit = logger.info("Container stopped [{}].", containerId)

}
