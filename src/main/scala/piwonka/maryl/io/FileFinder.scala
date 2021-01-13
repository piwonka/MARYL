package piwonka.maryl.io

import com.google.common.io.PatternFilenameFilter
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

object FileFinder{
  def find(root:Path, filter:PatternFilenameFilter, recursive:Boolean=true)(implicit fs:FileSystem):Seq[Path]={
    val files:Array[FileStatus] = fs.listStatus(root)
    val result = files.filter(f=>f.isFile).map(_.getPath).filter(p=>filter.accept(null,p.getName))//find all non-directory files that match the filter
    (result ++ files.filter(_.isDirectory).filter(_=>recursive).flatMap(f=>find(f.getPath(),filter,recursive))).toList
  }
}
