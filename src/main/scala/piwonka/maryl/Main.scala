package piwonka.maryl

import org.apache.hadoop.fs.Path
import piwonka.maryl.api.{MapReduceBuilder, MapReduceContext, YarnContext}

import scala.tools.nsc.io.File

object Main extends App {
  //Setup
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

  val comparer: ((String, Int), (String, Int)) => (String, Int) = (p1, p2) => if (Integer.parseInt(p1._1) < Integer.parseInt(p2._1)) p1 else p2
  val yarnContext = YarnContext(new Path(args(0)), "MarylApp")
  val inFile = new Path(args(1))
  val mapOutDir = Path.mergePaths(yarnContext.tempPath,new Path("/MapResults"))
  val copyDir = Path.mergePaths(yarnContext.tempPath,new Path("/CopyAndMerge"))
  val outFile = new Path(args(2))
  val context: MapReduceContext[Int, Int] =
    MapReduceContext(
      mapFunction,
      10000,
      0.8f,
      combineFunction,
      mapOutDir,
      copyDir,
      20000,
      0.8f,
      5,
      reduceFunction,
      inFile,
      outFile,
      inputParser,
      inputParser,
      outputParser,
      100,
      comparer)
  MapReduceBuilder.create(context, yarnContext).submit
}
