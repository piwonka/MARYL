package piwonka.maryl.mapreduce.reduce

object Reducer {
  final def reduce[T](f:(T,T)=>T,key:String,values:List[T]):(String,T) = {
    (key,reduceRec[T](f,values))
  }

  final private def reduceRec[T](f:(T,T)=>T,values:List[T]):T={
    if(values.tail==Nil) {
      return values.head
    }
    f(values.head,reduceRec(f,values.tail))
  }
}
