package piwonka.maryl.api
import piwonka.maryl.yarn.MARYLApp

object MapReduceBuilder {
  /**Creates a MARYLApp using the specifications of the supplied context.
   * @param mapRedContext All specifications concerning the MapReduce-Job
   * @param yarnContext All specifications concerning the underlying YARN-Application
   * @param cleanup specifies if intermediate data of the application should be deleted after the maryl app has finished
   * @return A MARYLApp, which can be submitted to start the Job*/
  def create[T, U](mapRedContext: MapReduceContext[T, U], yarnContext: YarnContext,cleanup:Boolean=true): MARYLApp[T,U] = {
    MARYLApp[T,U](yarnContext,mapRedContext,cleanup)
  }
}
