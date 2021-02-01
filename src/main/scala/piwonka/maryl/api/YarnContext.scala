package piwonka.maryl.api

import org.apache.hadoop.fs.Path
/**
 * @param localJarPath The path of the programs jarfile in the local filesystem
 * @param appName The application name
 * @param amMemory The RAM consumed by the ApplicationMaster
 * @param amCores The amount of vcores consumed by the ApplicationMaster
 * @param amPriority The priority at which the ApplicationMaster is scheduled
 * @param amQueue The queue the ApplicationMaster is scheduled to
 * @param tempPath The HDFS path where all generated Files of the operation are saved to
 * @param workerMemory The memory consumed by the Mappers and Reducers
 * @param workerCores The vcores consumed by the Mappers and Reducers
 * @param workerPriority The Priority the Mappers and Reducers are scheduled at
 */
case class YarnContext
(
  localJarPath: Path,
  appName: String = "MARYLApp",
  amMemory: Int = 256,
  amCores: Int = 1,
  amPriority: Int = 0,
  amQueue: String = "default",
  tempPath: Path = new Path("Maryl"),
  workerMemory: Int = 256,
  workerCores: Int = 256,
  workerPriority: Int = 1
) extends Serializable
