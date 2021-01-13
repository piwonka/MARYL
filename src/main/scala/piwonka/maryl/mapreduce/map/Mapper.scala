package piwonka.maryl.mapreduce.map

object Mapper {
  final def map[T,U](f:(String,T)=>List[(String,U)],key:String,value:T):List[(String,U)] = {
    f.apply(key,value)
  }
}
