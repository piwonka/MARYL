package piwonka.maryl.yarn

import java.io.File
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

/**
 * Contains all shared logic between ApplicationMaster and MARYLApp
 * @note This class takes inspiration from https://github.com/arunma/YarnAppWithScala/blob/master/src/main/scala/com/arunma/YarnAppUtils.scala
 */
object YarnAppUtils {
  /**
   * @param resource The Path of the file resource
   * @return A LocalResource-Object representing the needed file resource.
   */
  def setUpLocalResourceFromPath(resource: Path)(implicit conf: Configuration): LocalResource = {
    val status = FileSystem.get(conf).getFileStatus(resource)
    LocalResource.newInstance(
      URL.fromPath(resource, conf),
      LocalResourceType.FILE,
      LocalResourceVisibility.PUBLIC,
      status.getLen,
      status.getModificationTime)
  }

  /**
   * Sets up environmental variables of the container
   * @param addEnvVars Additional Environmental Variables
   * @return A HashMap containing all the environmental variables and their values
   */
  def buildEnvironment(addEnvVars: Map[String, String])(implicit conf: YarnConfiguration): Map[String, String] = {
    val classpath = conf.getTrimmedStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH: _*)
    val environment = new java.util.HashMap[String, String]()
    classpath.foreach(c => Apps.addToEnvironment(environment, Environment.CLASSPATH.name(), c, File.pathSeparator)) //Copy Classpath
    Apps.addToEnvironment(environment, Environment.CLASSPATH.name(), Environment.PWD.$() + File.pathSeparator + "*", File.pathSeparator) //Add Home to ClassPath
    addEnvVars.foreach { case (key, value) =>
      Apps.addToEnvironment(environment, key, value, File.pathSeparator)
    }
    environment.asScala.toMap
  }

  /**
   * Creates a ContainerLaunchContext from the supplied arguments, leaving blanks as necessary.
   * @param commands The launch command of the container
   * @param resources The local resources of the container
   * @param environment The container environment
   * @return ContainerLaunchContext
   */
  def createContainerContext(commands: List[String], resources: Map[String, LocalResource], environment: Map[String, String]): ContainerLaunchContext = {
    val jCommands = if(commands==null) null else commands.asJava
    val jResources = if(resources==null) null else resources.asJava
    val jEnvironment = if(environment==null) null else environment.asJava
    ContainerLaunchContext.newInstance(jResources, jEnvironment, jCommands, null, allTokens, null)
  }

  /**
   * This method was created to have MARYL work with kerberized clusters. It was not tested though.
   * @Note In the worst case this does nothing :)
   */
  private def allTokens: ByteBuffer = {
    // creating the credentials for container execution
    val credentials = UserGroupInformation.getCurrentUser.getCredentials
    val dob = new DataOutputBuffer
    credentials.writeTokenStorageToStream(dob)
    ByteBuffer.wrap(dob.getData, 0, dob.getLength)
  }

  /**
   * Serializes an Object and saves it in a hdfs file.
   * @param obj The object to be serialized
   * @param fileName The name of the File
   * @note All serialized objects are written to files located at tempPath, specified in the YarnContext
   */
  def serialize(obj: Serializable, fileName: String)(implicit yc: YarnContext, fc: FileContext): Unit = {
    val location: Path = fc.makeQualified(Path.mergePaths(yc.tempPath, new Path(s"/$fileName.obj")))
    val trySer = Using(fc.create(location, util.EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE))){ writer =>
      SerializationUtils.serialize(obj, writer)
    }
    if (trySer.isFailure) {
      trySer.failed.get.printStackTrace()
    }
  }

  /**
   * Deserializes an Object from a hdfs file.
   * @param location the file containing the object
   */
  def deserialize(location: Path): Any = {
    val trySer = Using(FileContext.getFileContext.open(location)) { reader =>
      SerializationUtils.deserialize[Any](reader)
    }
    if (trySer.isFailure) {
      trySer.failed.get.printStackTrace()
    }
    trySer.get
  }
}
