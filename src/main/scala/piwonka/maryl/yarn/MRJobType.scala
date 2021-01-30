package piwonka.maryl.yarn

object MRJobType extends Enumeration {
  type MRJobType = String
  val MAP = "piwonka.maryl.mapreduce.map.MapJob"
  val REDUCE = "piwonka.maryl.mapreduce.reduce.ReduceJob"
}
