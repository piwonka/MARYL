package piwonka.maryl.io

import java.io.InputStreamReader
import org.apache.hadoop.fs.{FileContext, Path}

/** Iterates over a specific block of a hdfs file.
 * Functionality is similar to FileIterator
 * @param file   The file that is being iterated over
 * @param parser A function that transforms input data to the desired format
 * @param blockId the id of the block that is being read
 **/
case class FileBlockIterator[T](file: Path, parser: String => T, blockId: Int)(implicit fc: FileContext) extends FileReader[T] {
  val blockLocations = fc.getFileBlockLocations(file, 0, fc.getFileStatus(file).getLen)
  var current = blockLocations(blockId).getOffset
  val end = current + blockLocations(blockId).getLength
  val input = {
    val fsInput = fc.open(file)
    fsInput.seek(current)
    new InputStreamReader(fsInput)
  }
  if (blockId != 0) println(s"Omitted:${readLine()}")

  private def readLine(): Option[String] = {
    val delimiters = Array('\n'.asInstanceOf[Int], '\r'.asInstanceOf[Int], -1)
    var c = input.read()
    current += 1
    var builder = new StringBuilder()
    while (!delimiters.contains(c)) {
      builder += c.asInstanceOf[Char]
      c = input.read()
      current += 1
    }
    if (builder.isEmpty) Option.empty else Some(builder.toString)
  }

  def next(): Option[T] = {
    try {
      val line = readLine()
      if (line.isDefined) {
        return Some(parser.apply(line.get))
      }
      Option.empty
    }
    catch {
      case e: Exception => e.printStackTrace(); Option.empty //Catches Parser Errors
    }
  }

  def hasNext: Boolean = {
    if (current <= end && !(current >= end && blockId == blockLocations.length - 1)) true
    else {
      input.close();
      false
    }
  }
}