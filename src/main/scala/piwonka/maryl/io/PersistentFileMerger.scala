package piwonka.maryl.io

import com.google.common.io.PatternFilenameFilter
import org.apache.hadoop.fs.{FileContext, FileSystem, Path}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

case class PersistentFileMerger[U](id: Int, fileCntPerMerge: Int, dir: Path, outputParser: U => String, inputParser: String => U, comparer: (U, U) => U)(implicit fs:FileSystem,fc:FileContext) {
  def merge(): FileMergingIterator[U] = {
    val files = if (fs.exists(dir)) FileFinder.find(dir, new PatternFilenameFilter("Spill.*.txt"))(fs) else Seq[Path]()
    if (files.length <= fileCntPerMerge) {
      return FileMergingIterator[U](inputParser,comparer, files)
    }
    println("Waiting for persistent Merge")
    val mergeResults = mergePersistent(files)
    FileMergingIterator[U](inputParser, comparer, mergeResults.filter(fs.exists(_)))
  }

  private def mergePersistent(files: Seq[Path], round: Int = 0): Seq[Path] = {
    if (files.length <= fileCntPerMerge) return files
    val result = (for (i <- 0 to (files.length / fileCntPerMerge)) yield Future[Path] {
      val currentIndex = i * fileCntPerMerge
      mergeN(files.slice(currentIndex, currentIndex + fileCntPerMerge), i, round)
    }).map(Await.result(_, Duration.Inf))
    mergePersistent(result, round + 1)
  }

  private def mergeN(files: Seq[Path], i: Int, round: Int): Path = {
    val mergeOutput = new Path(dir + "\\Reducer" + id + "_MergeOutput_" + i + "_Round_" + round + ".txt")
    val merger = FileMergingIterator[U](inputParser, comparer, files.filter(fs.exists(_)))
    val writer = TextFileWriter[U](mergeOutput, outputParser)
    merger.map(_.get).foreach(writer.write)
    mergeOutput
  }
}