package piwonka.maryl.mapreduce.map

/**
 * This class is used in conjunction with the map-function to generate the map outputs.
 * @note Technically this class is redundant because creating map outputs could be done by using the scala "map" function, passing the user function.
 *       This class exists for the sole purpose of making clear what is happening to the inputs.
 */
object Mapper {
  /**Transforms input data to the desired map outputs using the user-specified map function
   * @param fun the map function
   * @result A pair of (key,value) that has been transformed from the original input using the map function
   */
  final def map[T,U](fun:(String,T)=>List[(String,U)],key:String,value:T):List[(String,U)] = {
    fun(key,value)
  }
}
