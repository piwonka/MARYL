package piwonka.maryl.io

import java.io.InputStreamReader
import org.apache.hadoop.fs.{FileContext, Path}

case class FileBlockIterator[T](file: Path, parser: String => T, blockId: Int)(implicit fc: FileContext) extends FileReader[T] {
  val blockLocations = fc.getFileBlockLocations(file, 0, fc.getFileStatus(file).getLen)
  val start = blockLocations(blockId).getOffset
  var pointer = start
  val end = start + blockLocations(blockId).getLength
  val fsInput = fc.open(file)
  fsInput.seek(start)
  val input = new InputStreamReader(fsInput)
  if (blockId != 0) println(s"Omitted:${readLine()}")
  println(s"READER STARTPOSITION:${fsInput.getPos}")
  println(s"BLOCKLENGTH:${blockLocations(blockId).getLength}")

  var position = fsInput.getPos
  var pairsRead = 0

  private def readLine(): Option[String] = {
    val delimiters = Array('\n'.asInstanceOf[Int], '\r'.asInstanceOf[Int], -1)
    var c = input.read()
    pointer += 1
    var builder = new StringBuilder()
    while (!delimiters.contains(c)) {
      builder += c.asInstanceOf[Char]
      c = input.read()
      pointer += 1
    }
    position = fsInput.getPos
    if (builder.isEmpty) Option.empty else Some(builder.toString)
  }

  def next(): Option[T] = {
    try {
      val line = readLine()
      if (line.isDefined) {
        pairsRead += 1
        println(s"Read: ${line}|Pairs Read:$pairsRead|Position:${position}|realPointer:$pointer")
        return Some(parser.apply(line.get))
      }
      Option.empty
    }
    catch {
      case e: Exception => e.printStackTrace(); Option.empty //Catches Parser Errors
    }
  }

  def hasNext: Boolean = {
    if (pointer <= end && !(pointer >= end && blockId == blockLocations.length - 1)) true
    else {
      input.close();
      false
    }
  }
}
/*
* package piwonka.maryl.io

import java.io.InputStreamReader
import org.apache.hadoop.fs.{FileContext, Path}

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
  println(s"READER STARTPOSITION:${current}")
  println(s"BLOCKLENGTH:${blockLocations(blockId).getLength}")

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
* */