package piwonka.maryl.mapreduce

object MRJobType extends Enumeration{
  type MRJobType = String
  val MAP="piwonka.maryl.mapreduce.MapJob"
  val REDUCE = "piwonka.maryl.mapreduce.ReduceJob"
}
