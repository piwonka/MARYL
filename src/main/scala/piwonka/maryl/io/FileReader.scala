package piwonka.maryl.io

trait FileReader[T] extends Iterator[Option[T]] {
  val input:Any
  def next():Option[T]
  def hasNext:Boolean
}
