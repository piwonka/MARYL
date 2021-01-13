package piwonka.maryl.io

/**
 * Converts RemoteIterator from Hadoop to Scala Iterator that provides all the familiar functions such as map,
 * filter, foreach, etc.
 *
 * @param underlying The RemoteIterator that needs to be wrapped
 * @tparam T Items inside the iterator
 * @return Standard Scala Iterator
 *
 *
 * @note Source: https://gist.github.com/timvw/4ec727de9b76d9afc51298d9e68c4241
 */
case class RemoteIteratorWrapper[T](underlying: org.apache.hadoop.fs.RemoteIterator[T]) extends scala.collection.AbstractIterator[T] with scala.collection.Iterator[T] {
  def hasNext = underlying.hasNext
  def next() = underlying.next()
}

object Conversions {
  implicit def remoteIterator2ScalaIterator[T](underlying: org.apache.hadoop.fs.RemoteIterator[T]) : scala.collection.Iterator[T] = RemoteIteratorWrapper[T](underlying)
}
