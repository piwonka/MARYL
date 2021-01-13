package piwonka.maryl.api
import piwonka.maryl.yarn.MARYLYarnClient

object MapReduceBuilder {
  def create[T, U](mapRedContext: MapReduceContext[T, U], yarnContext: YarnContext): MARYLYarnClient = {
    MARYLYarnClient(yarnContext,mapRedContext)
  }
}
