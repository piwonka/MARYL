package piwonka.maryl.yarn

import org.apache.hadoop.yarn.api.records.{Container, ContainerLaunchContext}
import piwonka.maryl.yarn.MRJobType.MRJobType

/**
 * Wraps all information of a subtask
 * @param context The ContainerLaunchContext of the task, containing launch command, environment and local resource specifications
 */
case class Worker(context:ContainerLaunchContext,jobType: MRJobType){
  var container:Container=null
  var attempts = 0
}
