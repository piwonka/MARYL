package piwonka.maryl.mapreduce.reduce

import scala.annotation.tailrec

/***
 * This class is used in conjunction with the reduce-function to generate the reduce outputs.
 * @note Technically this class is redundant because creating reduce outputs could be done by using the scala "reduce" function, passing the user function.
 *       This class exists for the sole purpose of making clear what is happening to the inputs.
 */
object Reducer {

  /**
   * Calls reduceRec(f,values) to recursively reduce the value list
   * @param f The user specified reduce-function
   * @returns A Pair of (key,value) where value is the result of the reducing process
   */
  final def reduce[T](f:(T,T)=>T,key:String,values:List[T]):(String,T) = {
    (key,reduceRec[T](f,values.tail,values.head))
  }

  /**
   * Recursively reduces value list using the reduce function
   * @param f The user specified reduce-function
   * @returns The single value resulting from the reducing process
   */
  @tailrec
  final private def reduceRec[T](f:(T,T)=>T, values:List[T], res:T=null):T={
    if(res==null) reduceRec(f,values.tail,values.head)
    else if(values.isEmpty) res
    else reduceRec(f,values.tail,f(values.head,res))
  }
}
