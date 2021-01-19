package piwonka.maryl.yarn

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.yarn.api.records.{Container, ContainerId}
import piwonka.maryl.mapreduce.MRJobType
import piwonka.maryl.mapreduce.MRJobType.MRJobType

case class Worker(jobType: MRJobType){
  var container:Container=null
  var jobId:Int=0
  var attempts=0
}
