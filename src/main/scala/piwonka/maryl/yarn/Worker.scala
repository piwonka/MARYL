package piwonka.maryl.yarn

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.yarn.api.records.{Container, ContainerId, ContainerLaunchContext}
import MRJobType.MRJobType

case class Worker(context:ContainerLaunchContext,jobType: MRJobType){
  var container:Container=null
  var attempts = 0
}
