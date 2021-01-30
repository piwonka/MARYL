package piwonka.maryl.api

import org.apache.hadoop.fs.Path

import scala.reflect.runtime.universe._

/**
 * @param mapFunction              The function fed to the Mapper to transform data
 * @param spillBufferSize          The size of the spill buffer
 * @param spillThreshold           The percentage (0.00-1.00), the spill buffer needs to be filled before a spill is triggered
 * @param combineFunction          The function fed to the combiner to combine spill data
 * @param mapOutDir                The HDFS directory where Mapper Outputs are written to
 * @param copyTargetDir            The HDFS directory where Reducers save their intermediate results like copies of MapOutputs and merges
 * @param copyBufferSize           The size of the spill buffer during the copy phase
 * @param copyBufferSpillThreshold The spill threshold during the copy phase
 * @param reducerCount             The amount of reducers used
 * @param reduceFunction           The function fed to the Reducers to calculate the results
 * @param inputFile                The input of the MapReduce-Operations
 * @param outputFile               The file where the Reducer results are collected to
 * @param mapInputParser           The parser used to read mapper input data from string
 * @param reduceInputParser        the parser to read input data of the reducer from string
 * @param outputParser             the parser used to transform map and reduce outputs to string
 * @param mergeFactor              The maximum amount of Files that can be merged in-memory
 * @param comparer                 The function used to choose the smallest possible value when reading multiple Files with a FileMergingIterator
 **/
case class MapReduceContext[T, U]
(
  mapFunction: (String, T) => List[(String, U)],
  spillBufferSize: Int,
  spillThreshold: Float,
  combineFunction: (U, U) => U,
  mapOutDir: Path,
  copyTargetDir: Path,
  copyBufferSize: Int,
  copyBufferSpillThreshold: Float,
  reducerCount: Int,
  reduceFunction: (U, U) => U,
  inputFile: Path,
  outputFile: Path,
  mapInputParser: String => (String, T),
  reduceInputParser: String => (String, U),
  outputParser: ((String, U)) => String,
  mergeFactor: Int,
  comparer: ((String, U), (String, U)) => (String, U)) extends Serializable {
//(implicit tag: TypeTag[T], tag2: TypeTag[U])
  def this(map: (String, T) => List[(String, U)], reduce: (U, U) => U,inputFile:Path,outputFile:Path,
            mapInputParser: String => (String, T),reduceInputParser: String => (String, U),
            outputParser: ((String, U)) => String,comparer: ((String, U), (String, U)) => (String, U)) (implicit yc:YarnContext){
    this(
      map,
      100,
      0.8f,
      null,
      Path.mergePaths(yc.tempPath, new Path("/MapResults")),
      Path.mergePaths(yc.tempPath, new Path("/CopyAndMerge")),
      100,
      0.8f,
      1,
      reduce,
      inputFile,
      outputFile,
      mapInputParser,
      reduceInputParser,
      outputParser,
      100,
      comparer)
  }
  def apply(map: (String, T) => List[(String, U)], reduce: (U, U) => U,inputFile:Path,outputFile:Path,
            mapInputParser: String => (String, T),reduceInputParser: String => (String, U),
            outputParser: ((String, U)) => String,comparer: ((String, U), (String, U)) => (String, U)) (implicit yc:YarnContext):MapReduceContext[T,U] = {
            this(map,reduce,inputFile,outputFile,mapInputParser,reduceInputParser,outputParser,comparer)
  }
}
