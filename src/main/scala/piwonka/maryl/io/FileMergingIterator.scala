package piwonka.maryl.io

import org.apache.hadoop.fs.{FileContext, Path}
/**
 * An Iterator that reads line-by-line data from multiple pre-sorted files at the same time, always returning the smallest readable Value
 * @param fc The FileContext of the underlying HDFS
 * @param parser The parser to be invoked on raw file data
 * @param comparer The function that is used to compare parsed file data to determine the smallest value
 * @param files The Files to be read
 * */

case class FileMergingIterator[T](parser: String => T, comparer: (T, T) => T, files: Seq[Path])(implicit fc:FileContext) extends Iterator[Option[T]] with FileReader[T] {
  val readers = {for (file <- files) yield new FileIterator[T](file, parser)}.filter(_.hasNext).toArray //create Readers, close if empty
  val values = readers.map(_.next()) //read values from non.empty readers
  var nextVal: Option[T] = Option.empty

  /**
   * @return Returns an Option that contains the smallest readable value in files, if present, while NOT advancing the pointer any further
   * */
  def peek(): Option[T] = {
    if (!nextVal.isDefined) {
      nextVal = next()
    }
    nextVal
  }

  override def hasNext: Boolean = nextVal.isDefined || values.map(_.isDefined).foldLeft(false)(_ || _) //foldleft because can't reduce empty List


  /**
   * @return Returns an Option that contains the smallest readable value in files, if present, while advancing the pointer further
   * */
  override def next(): Option[T] = {
    if (nextVal.isDefined) {
      val tmp = nextVal
      nextVal = Option.empty
      return tmp
    }
    val smallestElement = values.filter(e => e.isDefined).reduce((v1, v2) => Some(comparer.apply(v1.get, v2.get)))
    val index = values.indexOf(smallestElement)
    values(index) = if (readers(index).hasNext) readers(index).next() else Option.empty
    smallestElement
  }

  /**
   * Reads KV-Pair data from files until the key changes, advancing the pointer until the new key would be next
   * @return Returns a pair of (key:K,values:List[V]), where values contains all values of the same key in files (assuming files were presorted)
   * */
  def groupBy[K, V](f: Option[T] => K)(g: Option[T] => V): (K, List[V]) = {
    val key = f.apply(peek())
    var values: List[V] = Nil
    while (hasNext && f.apply(peek()) == key) {
      values = g.apply(next()) :: values
    }
    (key, values)
  }
}
