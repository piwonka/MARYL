package piwonka.maryl.yarn

/**
 * Used to distinguish between map and reduce tasks. Aditionally used to easily create the job command.
 */
object MRJobType extends Enumeration {
  type MRJobType = String
  val MAP = "piwonka.maryl.mapreduce.map.MapJob"
  val REDUCE = "piwonka.maryl.mapreduce.reduce.ReduceJob"
}
