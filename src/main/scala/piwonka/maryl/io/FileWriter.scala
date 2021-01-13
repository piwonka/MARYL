package piwonka.maryl.io

trait FileWriter[T] {
  def write(t:T):Unit
}
