package piwonka.maryl

import org.apache.hadoop.fs.{FileContext, Path}
import piwonka.maryl.api.{MapReduceBuilder, MapReduceContext, YarnContext}
import piwonka.maryl.io.FileIterator
import piwonka.maryl.yarn.MARYLApp

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.tools.nsc.io.File

object Main extends App {
  def testStrings:Unit = {
    //Setup
    val mapFunction = (key: String, value: String) => {
      value.split(" ").groupBy(word => word).map(e => (e._1, e._2.length)).toList
    }
    val reduceFunction = (x: Int, y: Int) => x + y
    val combineFunction = reduceFunction

    def inputParser(line: String): (String, String) = {
      ("", line)
    }

    def reduceInputParser(line: String): (String, Int) = {
      val split = line.split(" ")
      val key: String = split(0)
      val value: String = split(1).trim
      (key, Integer.parseInt(value))
    }

    def outputParser(pair: (String, Int)): String = {
      val (key, value) = pair
      key + " " + Integer.toString(value)
    }

    val comparer: ((String, Int), (String, Int)) => (String, Int) = (p1, p2) => if (p1._1.charAt(0).asDigit < p2._1.charAt(0).asDigit) p1 else p2
    val yarnContext = YarnContext(new Path(args(0)), "MarylApp")
    val inFile = new Path(args(1))
    val mapOutDir = Path.mergePaths(yarnContext.tempPath, new Path("/MapResults"))
    val copyDir = Path.mergePaths(yarnContext.tempPath, new Path("/CopyAndMerge"))
    val outFile = new Path(args(2))

    val context: MapReduceContext[String, Int] =
      MapReduceContext(
        mapFunction,
        100,
        0.8f,
        combineFunction,
        mapOutDir,
        copyDir,
        150,
        0.8f,
        1,
        reduceFunction,
        inFile,
        outFile,
        inputParser,
        reduceInputParser,
        outputParser,
        100,
        comparer)
    val path:Future[FileIterator[(Any, Any)]] = MapReduceBuilder.create(context, yarnContext).submit
    Await.result(path, Duration.Inf).filter(_.isDefined).map(_.get).map(_.asInstanceOf[(String,Int)]).toList.sortBy(_._2).take(10).foreach(println)
  }

  def testNumbers:Unit = { //Setup
    val mapFunction = (key: String, x: Int) => List((key, x + 1))
    val reduceFunction = (x: Int, y: Int) => x + y
    val combineFunction = reduceFunction

    def inputParser(line: String): (String, Int) = {
      val split = line.split(" ")
      val key: String = split(0)
      val value: String = split(1).trim
      (key, Integer.parseInt(value))
    }

    def outputParser(pair: (String, Int)): String = {
      val (key, value) = pair
      key + " " + Integer.toString(value)
    }

    val yarnContext = YarnContext(new Path(args(0)), "MarylApp")
    val comparer: ((String, Int), (String, Int)) => (String, Int) = (p1, p2) => if (Integer.parseInt(p1._1) < Integer.parseInt(p2._1)) p1 else p2
    val inFile = new Path(args(1))
    val mapOutDir = Path.mergePaths(yarnContext.tempPath, new Path("/MapResults"))
    val copyDir = Path.mergePaths(yarnContext.tempPath, new Path("/CopyAndMerge"))
    val outFile = new Path(args(2))
    val context: MapReduceContext[Int, Int] =
      MapReduceContext(mapFunction, 10000, 0.8f, combineFunction, mapOutDir, copyDir, 20000, 0.8f, 5,
        reduceFunction, inFile, outFile, inputParser, inputParser, outputParser, 100, comparer)
    val application = MapReduceBuilder.create(context, yarnContext)
    application.submit() //Erst hier beginnt die Interaktion mit dem Hadoop-Cluster
  }



  def anschaulich = {
    //Spezifizierung der notwendigen Funktionen
    val mapFun = (key: String, value: String) => {
      value.split(" ").map((_, 1)).toList
    }
    val redFun   = (x: Int, y: Int) =>x + y
    val inParser =(line: String)=>("",line)
    val outParser=(pair:(String, Int))=>pair._1+" " +Integer.toString(pair._2)
    val comparer = (p1: (String, Int), p2: (String, Int)) => {
      if (p1._1.charAt(0) < p2._1.charAt(0)) p1 else p2
    }
    val redInParser = (line: String) => {
      val split = line.split(" ")
      (split(0), Integer.parseInt(split(1).trim))
    }
    val inFile = new Path("in/rotk.txt")
    val outFile = new Path("out/rotk.txt")
    //Erzeugen der Kontext-Objekte
    implicit val yc: YarnContext = YarnContext(new Path(args(0)), "WordCount")
    val mrc = new MapReduceContext(mapFun, redFun, inFile, outFile, inParser,
                                    redInParser, outParser, comparer)
    //Erzeugen und starten der MARYL-Applikation
    val app: MARYLApp[String,Int] = MapReduceBuilder.create(mrc,yc)
    val appResult: Future[FileIterator[(String,Int)]] = app.submit()
    Await.result(appResult, Duration.Undefined)
  }
  testStrings
}
