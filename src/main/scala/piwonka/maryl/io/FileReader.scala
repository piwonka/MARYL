package piwonka.maryl.io

trait FileReader[T] {
  def next():Option[T]
  def hasNext:Boolean
  def peek():Option[T]
}
