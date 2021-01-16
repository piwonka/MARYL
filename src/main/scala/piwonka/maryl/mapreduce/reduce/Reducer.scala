package piwonka.maryl.mapreduce.reduce

import scala.annotation.tailrec

object Reducer {
  final def reduce[T](f:(T,T)=>T,key:String,values:List[T]):(String,T) = {
    (key,reduceRec[T](f,values.tail,values.head))
  }

  @tailrec
  final private def reduceRec[T](f:(T,T)=>T, values:List[T], res:T=null):T={
    if(res==null) reduceRec(f,values.tail,values.head)
    else if(values.isEmpty) res
    else reduceRec(f,values.tail,f(values.head,res))
  }
}
