package piwonka.maryl.yarn

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, FileSystem, Path}
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.{YarnClient, YarnClientApplication}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import piwonka.maryl.api.{MapReduceContext, YarnContext}
import piwonka.maryl.io.FileIterator
import piwonka.maryl.yarn.YarnAppUtils._

import scala.Console.println
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters.MapHasAsJava

/**
 * A Class representing the state of a MARYL application. Instances can be started to distribute a mapreduce-job on the cluster.
 * @note This object functions as a driver-class, its name does not reflect that, to make the API more clear
 * @param yarnContext An object wrapping all parameters used to influence yarn-application starts
 * @param mapReduceContext An object wrapping all parameters used to influence the map/reduce-tasks
 * @tparam T Type of the values contained in mapper output pairs
 * @tparam U Type of the values contained in reducer output pairs
 */
case class MARYLApp[T,U](yarnContext: YarnContext, mapReduceContext:MapReduceContext[T,U],cleanup:Boolean) {
  private implicit val conf:YarnConfiguration = new YarnConfiguration()
  private implicit val fs:FileSystem = FileSystem.get(conf)
  private implicit val fc:FileContext = FileContext.getFileContext(fs.getUri)
  private implicit val yc:YarnClient = YarnClient.createYarnClient()
  private val context:ContainerLaunchContext = createAMContainerLaunchContext

  /**
   * @return A ContainerLaunchContext representing the environment, resources and launch command of the ApplicationMaster
   */
  private def createAMContainerLaunchContext:ContainerLaunchContext={
    //----------COMMAND---------
    val commands:List[String] = List(
      "$JAVA_HOME/bin/java " +
        s" -Xmx${yarnContext.amMemory}m " +
        s" piwonka.maryl.yarn.MARYLApplicationMaster " +
        " 1> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
        " 2> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
    )
    //--------RESOURCES---------
    //SKIPPED HERE TO KEEP THIS FUNCTION STATELESS (Resources are uploaded and set in submit()
    //----------ENVIRONMENT---------
    val addEnvVars:Map[String,String] =
      Map(
        "HDFSJarPath" -> fs.makeQualified(Path.mergePaths(yarnContext.tempPath,new Path("/MarylApp.jar"))).toString,
        "MRContext" -> fs.makeQualified(Path.mergePaths(yarnContext.tempPath,new Path("/MRContext.obj"))).toString,
        "YarnContext" ->fs.makeQualified(Path.mergePaths(yarnContext.tempPath,new Path("/YarnContext.obj"))).toString
        )
    val environment:Map[String,String] = buildEnvironment(addEnvVars)
    //set up amContainer
    val amContainer:ContainerLaunchContext = createContainerContext(commands,null,environment)
    amContainer
  }

  /**
   * Starts the process of submitting the application, beginning all stateful operations
   * @return A future that will complete once the distributed application has finished, returning an iterator of the output file
   */
  def submit():Future[FileIterator[(String,U)]]= {
    //upload the Jar and add to AM-ContainerLaunchContext
    println("uploading JAR-file...")
    val localResources:Map[String,LocalResource] =
      Map("MarylApp.jar" -> uploadFile(yarnContext.localJarPath,Path.mergePaths(yarnContext.tempPath,new Path("/MarylApp.jar"))))
      context.setLocalResources(localResources.asJava)
    //Upload Contexts
    println("serializing contexts...")
    serialize(yarnContext,"YarnContext")(yarnContext,fc)
    serialize(mapReduceContext,"MRContext")(yarnContext,fc)
    //Start Yarn Client
    println("")
    yc.init(conf)
    yc.start()

    //Set up Application Launch Context
    val application:YarnClientApplication =yc.createApplication()
    val appContext:ApplicationSubmissionContext = application.getApplicationSubmissionContext
    appContext.setAMContainerSpec(context)
    appContext.setApplicationName(yarnContext.appName)
    appContext.setResource(Resource.newInstance(yarnContext.amMemory, yarnContext.amCores))
    appContext.setPriority(Priority.newInstance(yarnContext.amPriority))
    appContext.setQueue(yarnContext.amQueue)
    appContext.setKeepContainersAcrossApplicationAttempts(false)//because containers should keep datalocality and cant be easily identified, it is better to just replace them when the application master fails.
    //submit Application to Scheduler
    yc.submitApplication(appContext)

    //return Future of result
    val appId:ApplicationId = appContext.getApplicationId
    Future{
      var appReport: ApplicationReport = yc.getApplicationReport(appId)
      var appState: YarnApplicationState = appReport.getYarnApplicationState
      //Wait for finish
      while (!((appState eq YarnApplicationState.FINISHED) || (appState eq YarnApplicationState.FAILED) || (appState eq YarnApplicationState.KILLED))) {
        Thread.sleep(1000)
        val newAppReport = yc.getApplicationReport(appId)
        val newAppState = appReport.getYarnApplicationState
        if(!newAppState.equals(appState)){
          println(appReport.getName +s"[$appId] currently in state $appState")
        }
        appReport = newAppReport
        appState = newAppState
      }
      println(appReport.getName +"["+appId + "] finished with state " + appState + " in " + (appReport.getFinishTime-appReport.getStartTime)+"ms")
      if(cleanup) deleteTempFiles()
      if(appState==YarnApplicationState.FINISHED) FileIterator(mapReduceContext.outputFile,mapReduceContext.reduceInputParser) else null
    }
  }

  private def uploadFile(source:Path, dest:Path)(implicit conf:Configuration, fs:FileSystem):LocalResource={
    //set up Path of Jar in HDFS
    val destPath:Path = fs.makeQualified(dest)
    //upload Jar to HDFS
    fs.copyFromLocalFile(false, true, source, destPath)
    setUpLocalResourceFromPath(destPath)
  }

  private def deleteTempFiles(): Unit ={
    fs.delete(yarnContext.tempPath,true)
  }
}
