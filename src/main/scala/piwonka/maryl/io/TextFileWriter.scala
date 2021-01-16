package piwonka.maryl.io

import java.io.{BufferedWriter, OutputStreamWriter}
import java.util

import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.fs.{CreateFlag, FileContext, FileSystem, Path}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.yarn.conf.YarnConfiguration

/** Wraps a BufferedWriter and parses data into output format
 * Creates file if file does not exist, otherwise appends
 *
 * @param fc     The FileContext of the underlying HDFS
 * @param file   The file that is being written to
 * @param parser A function that transforms data to the desired output format
 **/
case class TextFileWriter[T](file: Path, parser: T => String)(implicit fc: FileContext) extends FileWriter[T] {
  val openedFile: BufferedWriter = new BufferedWriter(new OutputStreamWriter(fc.create(file, util.EnumSet.of(CreateFlag.CREATE, CreateFlag.APPEND))))

  /** Writes data to the Hadoop Filesystem
   *
   * @params t The Data that is supposed to be written to the HDFS
   **/
  def write(t: T): Unit = {
    openedFile.write(parser(t) + "\n")
  }

  def close(): Unit = {
    openedFile.close()
  }
}