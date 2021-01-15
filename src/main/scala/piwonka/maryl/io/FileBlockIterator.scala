package piwonka.maryl.io

import java.io.{BufferedReader, IOException, InputStreamReader}

import org.apache.hadoop.fs.{FileContext, Path}

case class FileBlockIterator[T](file: Path, parser: String => T, blockId:Int)(implicit fc: FileContext) extends FileReader[T] {
  override val input = {
    val blockIn = fc.open(file)
    blockIn.seek(blockId * blockSize)
    val blockReader = new BufferedReader(new InputStreamReader(blockIn))
    if(blockId!=0) blockReader.readLine() //drump a line because every block except block 1
    blockReader
  }
  val blockSize = fc.getFileStatus(file).getBlockSize
  var remaining = blockSize
  override var nextVal: Option[T] = Option.empty[T]

  override def next(): Option[T] = {
    if (nextVal.isDefined) {
      val tmp: Option[T] = nextVal
      nextVal = Option.empty
      return tmp
    }
    try {
      val nextInput = Option(input.readLine())
      if (nextInput.isDefined){
        remaining -= nextInput.get.getBytes.length
        Some(parser.apply(nextInput.get))}
      else Option.empty
    }
    catch {
      case e: Exception => e.printStackTrace(); Option.empty //Catches Parser Errors
    }
  }

  override def hasNext: Boolean = if (peek().isDefined&&remaining>0) true else {
    input.close();
    false
  }

  override def peek(): Option[T] = {
    if (!nextVal.isDefined) {
      nextVal = next()
    }
    nextVal
  }
}
