package piwonka.maryl.api
import piwonka.maryl.yarn.MARYLApp

object MapReduceBuilder {
  /**Creates a MARYLApp using the specifications of the supplied context.
   * @param mapRedContext All specifications concerning the MapReduce-Job
   * @param yarnContext All specifications concerning the underlying YARN-Application
   * @return a MARYLApp, which can be submitted to start the Job*/
  def create[T, U](mapRedContext: MapReduceContext[T, U], yarnContext: YarnContext): MARYLApp[T,U] = {
    MARYLApp[T,U](yarnContext,mapRedContext)
  }
}
