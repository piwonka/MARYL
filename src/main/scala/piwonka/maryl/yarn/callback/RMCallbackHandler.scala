package piwonka.maryl.yarn.callback

import java.util

import org.apache.hadoop.yarn.api.records.{Container, ContainerStatus, NodeReport, UpdatedContainer}
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.slf4j.LoggerFactory
import piwonka.maryl.mapreduce.DistributedMapReduceOperation

import scala.jdk.CollectionConverters.ListHasAsScala

case class RMCallbackHandler(dmro:DistributedMapReduceOperation) extends AMRMClientAsync.AbstractCallbackHandler {
  private val logger = LoggerFactory.getLogger(classOf[RMCallbackHandler])
  override def onContainersCompleted(statuses:util.List[ContainerStatus]): Unit = {
    logger.info("Containers completed: [{}].", statuses)
    statuses.asScala.foreach(c=>dmro.markTaskCompleted(c.getContainerId))
  }

  override def onContainersAllocated(containers: util.List[Container]): Unit = {
    logger.info("Container allocated [{}].", containers)
    dmro.receiveContainers(containers.asScala)
  }

  override def onContainersUpdated(containers:util.List[UpdatedContainer]): Unit = logger.info("Container updated [{}].", containers)

  override def onShutdownRequest(): Unit = logger.info("Shutdown requested.")

  override def onNodesUpdated(updatedNodes: util.List[NodeReport]): Unit = logger.info("Nodes updated [{}].", updatedNodes)

  override def getProgress:Float = 100f

  override def onError(error: Throwable): Unit = logger.error("Unknown error.", error)
}