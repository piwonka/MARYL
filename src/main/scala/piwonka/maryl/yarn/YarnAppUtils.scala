package piwonka.maryl.yarn

import java.io.{File, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer
import java.util

import org.apache.commons.lang3.SerializationUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{CreateFlag, FileContext, FileSystem, Path}
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Apps
import piwonka.maryl.api.YarnContext

import scala.jdk.CollectionConverters.{MapHasAsJava, MapHasAsScala, SeqHasAsJava}
import scala.util.Using

object YarnAppUtils {

  def setUpLocalResourceFromPath(resource: Path)(implicit conf: Configuration): LocalResource = {
    val status = FileSystem.get(conf).getFileStatus(resource)
    LocalResource.newInstance(
      URL.fromPath(resource, conf),
      LocalResourceType.FILE,
      LocalResourceVisibility.PUBLIC,
      status.getLen,
      status.getModificationTime)
  }

  def buildEnvironment(addlEnv: Map[String, String])(implicit conf: YarnConfiguration): Map[String, String] = {
    val classpath = conf.getTrimmedStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH: _*)
    val environment = new java.util.HashMap[String, String]()
    classpath.foreach(c => Apps.addToEnvironment(environment, Environment.CLASSPATH.name(), c, File.pathSeparator)) //Copy Classpath
    println("ENV PWD: ", Environment.PWD.$)
    Apps.addToEnvironment(environment, Environment.CLASSPATH.name(), Environment.PWD.$() + File.pathSeparator + "*", File.pathSeparator) //Add Home to ClassPath
    addlEnv.foreach { case (key, value) =>
      Apps.addToEnvironment(environment, key, value, File.pathSeparator)
    } //Add JarPath to Env
    environment.asScala.foreachEntry(println(_, _))
    environment.asScala.toMap
  }

  def createContainerContext(commands: List[String], resources: Map[String, LocalResource], environment: Map[String, String]): ContainerLaunchContext = {
    val jCommands = try {
      commands.asJava
    } catch {
      case _: Exception => null
    } //#todo: check if works with Some(e).getOrElse(null)
    val jResources = try {
      resources.asJava
    } catch {
      case _: Exception => null
    }
    val jEnvironment = try {
      environment.asJava
    } catch {
      case _: Exception => null
    }
    val context: ContainerLaunchContext = ContainerLaunchContext.newInstance(jResources, jEnvironment, jCommands, null, allTokens, null)
    context
  }

  private def allTokens: ByteBuffer = {
    // creating the credentials for container execution
    val credentials = UserGroupInformation.getCurrentUser.getCredentials
    val dob = new DataOutputBuffer
    credentials.writeTokenStorageToStream(dob)
    ByteBuffer.wrap(dob.getData, 0, dob.getLength)
  }

  def serialize(obj: Serializable, fileName: String)(implicit yc: YarnContext, fc: FileContext): Unit = {
    println("Serialize")
    val location: Path = fc.makeQualified(Path.mergePaths(yc.tempPath, new Path(s"/$fileName.obj")))
    val test = Using(fc.create(location, util.EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE))){ writer =>
      SerializationUtils.serialize(obj, writer)
      //writer.hsync()
    }
    if (test.isFailure) {
      test.failed.get.printStackTrace()
      System.exit(444)
    }
    println(s"Wrote $location")
  }

  def deserialize(location: Path): Any = {
    println("Deserialize")
    val test = Using(FileContext.getFileContext.open(location)) { reader =>
      SerializationUtils.deserialize[Any](reader)
    }
    if (test.isFailure) {
      test.failed.get.printStackTrace()
      System.exit(444)
    }
    test.get
  }
}
