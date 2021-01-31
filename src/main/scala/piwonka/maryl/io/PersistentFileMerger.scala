package piwonka.maryl.io

import com.google.common.io.PatternFilenameFilter
import org.apache.hadoop.fs.{FileContext, FileSystem, Path}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
/**
 * @param id The Id of the calling Reducer
 * @param mergeFactor The maximum amount of files that are permitted to be read from on parallel without merging them persistently
 * @param dir The path where mergeresults are saved
 * @param comparer The function that is used to compare parsed file data to determine the smallest value when merging
 * @param inputParser The parser used to read data from the merge input
 * @param outputParser The parser used to transform data into a string when writing the merge output
 * */
case class PersistentFileMerger[U](id: Int, mergeFactor: Int, dir: Path, outputParser: U => String, inputParser: String => U, comparer: (U, U) => U)(implicit fs:FileSystem, fc:FileContext) {

  /**
   * Checks the copied files and merges them persistently, when needed
   * @return A FileMergingIterator that is used to merge remaining data in-memory
   */
  def merge(): FileMergingIterator[U] = {
    val files = if (fs.exists(dir)) FileFinder.find(dir, new PatternFilenameFilter("Spill.*.txt"))(fs) else Seq[Path]() //Find all spillfiles in the current working directory
    if (files.length <= mergeFactor) {
      return FileMergingIterator[U](inputParser,comparer, files)//only in memory merging is done
    }
    val mergeResults = mergePersistent(files)
    FileMergingIterator[U](inputParser, comparer, mergeResults.filter(fs.exists(_)))
  }

  /**
   * Persistently merges files through multiple merge rounds.
   * merges are performed on file groups in parallel.
   * Stops merging when the amount of files falls below mergefactor.
   * @returns A list of merge-Results with a length < mergeFactor
   * */
  private def mergePersistent(files: Seq[Path], round: Int = 0): Seq[Path] = {
    if (files.length <= mergeFactor) return files
    //split files in groups and merge them in parallel
    val result = (for (i <- 0 to (files.length / mergeFactor)) yield Future[Path] {
      val currentIndex = i * mergeFactor
      mergeN(files.slice(currentIndex, currentIndex + mergeFactor), i, round)
    }).map(Await.result(_, Duration.Inf))
    mergePersistent(result, round + 1)//repeat until recursion anchor is met
  }

  /**
  *Persistently merges Groups of files through a FileMergingIterator
  *@returns The file resulting from the merge
  */
  private def mergeN(files: Seq[Path], i: Int, round: Int): Path = {
    val mergeOutput = new Path(dir + "\\Reducer" + id + "_MergeOutput_" + i + "_Round_" + round + ".txt") //create resultfile
    val merger = FileMergingIterator[U](inputParser, comparer, files.filter(fs.exists(_)))
    val writer = TextFileWriter[U](mergeOutput, outputParser)
    merger.map(_.get).foreach(writer.write)
    writer.close()
    mergeOutput
  }
}