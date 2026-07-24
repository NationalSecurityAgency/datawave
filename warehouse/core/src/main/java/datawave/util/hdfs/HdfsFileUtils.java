package datawave.util.hdfs;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsFileUtils {
  
  /**
   * Utility for opening a HDFS file when the
   * FileStatus object for the associated file
   * has already been retrieved from the NameNode.
   * 
   * HDFS-17593 added an optimization to FileSystem.openFile
   * that will use the block locations in the FileStatus
   * object in the client to reduce RPC calls to the
   * NameNode.
   * 
   * @param fs Hadoop FileSystem object
   * @param path Path to file
   * @param status FileStatus object for the file
   * @return stream for reading file
   * @throws IOException
   */
  public static FSDataInputStream openFile(FileSystem fs, Path path, FileStatus status)
      throws IOException {

    Objects.requireNonNull(fs);
    Objects.requireNonNull(path);
    Objects.requireNonNull(status);
    
    final CompletableFuture<FSDataInputStream> future = fs.openFile(path).withFileStatus(status).build();
    while (!future.isDone()) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while opening file: " + path, e);
      }
    }
    try {
      return future.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while opening file: " + path, e);
    } catch (CancellationException e) {
      throw new IOException("Cancelled while opening file: " + path, e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw new IOException("Error trying to open file: " + path, e);
    }
  }

}
