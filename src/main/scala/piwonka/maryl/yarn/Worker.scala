package piwonka.maryl.yarn

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.yarn.api.records.{Container, ContainerId}
import MRJobType.MRJobType

case class Worker(jobType: MRJobType){
  var container:Container=null
  var jobId:Int= -1
  var attempts = 0
}
