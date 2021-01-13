package piwonka.maryl.mapreduce.combine

import piwonka.maryl.mapreduce.reduce.Reducer

object Combiner{
  def combine[T](combineFunction:(T,T)=>T,key: String, values: List[T]): (String, T) = {
   //Because of the similar functionality of reduce and combine i hereby deduce that i will reuse reduce...heh
    Reducer.reduce(combineFunction, key, values)
  }

}
