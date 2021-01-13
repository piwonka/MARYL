package piwonka.maryl.io

import java.io.{BufferedReader, IOException, InputStreamReader}

import org.apache.hadoop.fs.{FileContext, Path}

/**
 * Simple Iterator, that wraps a FSDataInputStream and parses TextInput
 * @param fc The FileContext of the underlying HDFS
 * @param file The file that is being read
 * @param parser The function that is used to parse the file input to the desired type
 * */
case class FileIterator[T](file:Path, parser:String=>T)(implicit fc:FileContext) extends Iterator[Option[T]] with FileReader[T]{
  val openedFile:BufferedReader= new BufferedReader(new InputStreamReader(fc.open(file)))
  var nextVal:Option[T] = Option.empty

  /**
   * @return An Option[T] that contains the next value of the file if present and does NOT advance the pointer any further
   * */
  def peek():Option[T] = {
    if(!nextVal.isDefined){
      nextVal=next()
    }
    nextVal
  }

  override def hasNext: Boolean = if(peek().isDefined) true else{openedFile.close();false}
  /**
   * @return An Option[T] that contains the next value of the file if present and advances the pointer further
   * */
  override def next():Option[T] = {
    if(nextVal.isDefined){
      val tmp:Option[T] = nextVal
      nextVal = Option.empty
      return tmp
    }
    try{
      val nextInput = Option(openedFile.readLine())
      if(nextInput.isDefined) Some(parser.apply(nextInput.get))
      else Option.empty
    }
    catch{
      case e:IOException=>Option.empty//Catches IOException: Stream closed() #todo:Split up stream for multiple reads on Mappers
      case e:Exception=>e.printStackTrace();Option.empty//Catches everything that is not text-data
    }
  }
}
