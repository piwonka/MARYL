package piwonka.maryl.io

import java.io.BufferedReader

trait FileReader[T] extends Iterator[Option[T]] {
  val input:BufferedReader
  var nextVal:Option[T]
  def next():Option[T]
  def hasNext:Boolean
  def peek():Option[T]
}
