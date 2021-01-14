package piwonka.maryl.yarn

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, FileSystem, Path}
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.{YarnClient, YarnClientApplication}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.slf4j.LoggerFactory
import piwonka.maryl.api.{MapReduceContext, YarnContext}
import piwonka.maryl.io.RemoteIteratorWrapper
import piwonka.maryl.yarn.YarnAppUtils._

import scala.Console.println
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters.MapHasAsJava


case class MARYLYarnClient(yarnContext: YarnContext,mapReduceContext:MapReduceContext[_,_]) {
  private val logger = LoggerFactory.getLogger(classOf[MARYLYarnClient])
  implicit val conf:YarnConfiguration = new YarnConfiguration()
  implicit val fs:FileSystem = FileSystem.get(conf)
  implicit val fc:FileContext = FileContext.getFileContext(fs.getUri)
  implicit val yc:YarnClient = YarnClient.createYarnClient()
  val context:ContainerLaunchContext = createAMContainerLaunchContext

  private def createAMContainerLaunchContext:ContainerLaunchContext={
    //----------COMMAND---------
    val commands:List[String] = List(
      "$JAVA_HOME/bin/java " +
        s" -Xmx${yarnContext.amMemory}m " +
        s" piwonka.maryl.mapreduce.DistributedMapReduceOperation " +
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
    println(addEnvVars)
    val environment:Map[String,String] = buildEnvironment(addEnvVars)
    //set up amContainer
    val amContainer:ContainerLaunchContext = createContainerContext(commands,null,environment)
    amContainer
  }

  def submit:Future[Path]= {
    //upload the Jar and add to AM-ContainerLaunchContext
    val localResources:Map[String,LocalResource] =
      Map("MarylApp.jar" -> uploadFile(yarnContext.localJarPath,Path.mergePaths(yarnContext.tempPath,new Path("/MarylApp.jar"))))
      context.setLocalResources(localResources.asJava)
    println(localResources)
    //Upload Contexts
    serialize(yarnContext,"YarnContext")(yarnContext,fc)
    serialize(mapReduceContext,"MRContext")(yarnContext,fc)
    RemoteIteratorWrapper(fs.listFiles(yarnContext.tempPath,true)).foreach(println(_))
    //Start Yarn Client
    yc.init(conf)
    yc.start()

    //Set up Application Launch Context
    val application:YarnClientApplication =yc.createApplication()
    val appContext:ApplicationSubmissionContext = application.getApplicationSubmissionContext
    appContext.setAMContainerSpec(context)
    appContext.setApplicationName(yarnContext.appName)
    appContext.setResource(Resource.newInstance(yarnContext.amMemory, yarnContext.amCores))//Test
    appContext.setPriority(Priority.newInstance(yarnContext.amPriority))
    appContext.setQueue(yarnContext.amQueue)

    //submit Application to Scheduler
    yc.submitApplication(appContext)

    //return Future of result
    val appId:ApplicationId = appContext.getApplicationId
    Future{
      var appReport: ApplicationReport = yc.getApplicationReport(appId)
      var appState: YarnApplicationState = appReport.getYarnApplicationState
      //Wait for finish
      while (!((appState eq YarnApplicationState.FINISHED) || (appState eq YarnApplicationState.FAILED) || (appState eq YarnApplicationState.KILLED))) {
        Thread.sleep(3000)
        appReport = yc.getApplicationReport(appId)
        appState = appReport.getYarnApplicationState
        println(appReport.getName +"["+appId + "] currently in state " + appState)
      }
      println(appReport.getName +"["+appId + "] finished with state " + appState + " in " + (appReport.getFinishTime-appReport.getStartTime)+"ms")
      //cleanup
      if(appState==YarnApplicationState.FINISHED) mapReduceContext.outputFile else null
    }
  }

  private def uploadFile(source:Path, dest:Path)(implicit conf:Configuration, fs:FileSystem):LocalResource={
    //set up Path of Jar in HDFS
    val destPath:Path = fs.makeQualified(dest)
    //upload Jar to HDFS
    fs.copyFromLocalFile(false, true, source, destPath)
    setUpLocalResourceFromPath(destPath)
  }

  private def cleanup: Unit ={
    fs.delete(yarnContext.tempPath,true)
  }
}
