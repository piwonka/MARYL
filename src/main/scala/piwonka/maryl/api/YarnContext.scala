package piwonka.maryl.api

import org.apache.hadoop.fs.{FileContext, FileSystem, Path}
import org.apache.hadoop.yarn.conf.YarnConfiguration
case class YarnContext
(
 localJarPath: Path,
 appName: String,
 amMemory:Int=256,
 amCores:Int=1,
 amPriority:Int=0,
 amQueue:String="default",
 tempPath:Path = new Path ("Maryl"),
 fs:FileSystem=FileSystem.get(new YarnConfiguration()),
 fc:FileContext = FileContext.getFileContext
)
