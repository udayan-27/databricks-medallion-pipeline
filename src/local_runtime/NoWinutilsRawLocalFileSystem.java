package dec1.localfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Local-Windows FileSystem that does not call winutils.exe or Hadoop NativeIO.
 *
 * Spark 3.5.6 ships Hadoop 3.3.4. On Windows that library:
 * - mkdirs -&gt; setPermission -&gt; winutils.exe chmod
 * - parquet FileOutputCommitter -&gt; listStatus -&gt; NativeIO$Windows.access0
 *
 * This class is compiled at local SparkSession start and installed only as
 * {@code fs.file.impl}. Databricks uses the cluster session and never loads it.
 * Do not treat this as a production Hadoop distribution.
 */
public class NoWinutilsRawLocalFileSystem extends RawLocalFileSystem {
  @Override
  public void setPermission(Path p, FsPermission permission) throws IOException {
    // no-op: skip winutils chmod
  }

  @Override
  public void setOwner(Path p, String username, String groupname) throws IOException {
    // no-op
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    File path = pathToFile(f);
    if (!path.exists()) {
      throw new FileNotFoundException("File " + f + " does not exist.");
    }
    return new FileStatus(
        path.length(),
        path.isDirectory(),
        1,
        getDefaultBlockSize(f),
        path.lastModified(),
        makeQualified(f));
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    File localf = pathToFile(f);
    if (!localf.exists()) {
      throw new FileNotFoundException("File " + f + " does not exist.");
    }
    if (localf.isFile()) {
      return new FileStatus[] {getFileStatus(f)};
    }
    String[] names = localf.list();
    if (names == null) {
      return new FileStatus[0];
    }
    FileStatus[] results = new FileStatus[names.length];
    for (int i = 0; i < names.length; i++) {
      results[i] = getFileStatus(new Path(f, names[i]));
    }
    return results;
  }
}
