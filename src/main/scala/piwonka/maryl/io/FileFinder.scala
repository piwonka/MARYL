package piwonka.maryl.io

import com.google.common.io.PatternFilenameFilter
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
/** Used to scan the directory structure of a root path for matching filenames.
 **/
object FileFinder{
  def find(root:Path, filter:PatternFilenameFilter, recursive:Boolean=true)(implicit fs:FileSystem):Seq[Path]={
    val files:Array[FileStatus] = fs.listStatus(root)//list files in root
    //find all non-directory files that match the filter:
    val result = files.filter(f=>f.isFile).map(_.getPath).filter(p=>filter.accept(null,p.getName))
    //repeat recursively for all directories in the current folder when recursive =true then return:
    (result ++ files.filter(_.isDirectory).filter(_=>recursive).flatMap(f=>find(f.getPath(),filter,recursive))).toList }
}
