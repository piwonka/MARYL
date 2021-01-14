package piwonka.maryl.api

import org.apache.hadoop.fs.Path

import scala.reflect.runtime.universe._

case class MapReduceContext[T, U]
(
  mapperCount: Int,
  mapFunction: (String, T) => List[(String, U)],
  spillBufferSize: Int,
  spillThreshold: Float,
  combineFunction: (U, U) => U,
  mapOutDir: Path,
  copyDir: Path,
  copyBufferSize:Int,
  copyBufferSpillThreshold:Float,
  reducerCount: Int,
  reduceFunction: (U, U) => U,
  inputFile: Path,
  outputFile: Path,
  inputParser: String => (String, T),
  reduceInputParser: String => (String, U),
  outputParser: ((String,U)) => String,
  fileCntPerMerge: Int,
  pairComparer:((String,U),(String,U))=>(String,U))(implicit tag:TypeTag[T],tag2:TypeTag[U]) extends Serializable
