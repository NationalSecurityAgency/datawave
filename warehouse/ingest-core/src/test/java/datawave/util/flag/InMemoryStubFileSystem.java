package datawave.util.flag;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A Test FileSystem that provides an in-memory listing of file status objects
 */
public class InMemoryStubFileSystem extends FileSystem {
    private static final int FILE_LENGTH = 0;
    static final int BLOCK_SIZE = 245;
    static final long MODIFICATION_TIME = 123L;
    private final Map<Path,LocatedFileStatus[]> pathToListStatus = new HashMap<>();
    private final Map<Path,LocatedFileStatus> pathToFileStatus = new HashMap<>();
    private Path workingDirectory;
    private final URI uri;
    private final String scheme;

    public InMemoryStubFileSystem(String scheme) {
        this.scheme = scheme;
        this.uri = URI.create(this.scheme + ":///");
        this.workingDirectory = new Path(this.uri.toString());

        super.setConf(new Configuration());
    }

    /**
     * Creates in-memory a new LocatedFileStatus stub and stubs for all the directories in its path
     *
     * @param relativePathStr
     *            path as string
     */
    public void addFile(String relativePathStr) {
        Path path = new Path(this.getWorkingDirectory(), relativePathStr);

        Path parentPath = createAncestorDirectories(path);

        // create a simulated file
        LocatedFileStatus fileStatus = stubFileStatus(path);

        // ensure listings on the parent directory include the file
        pathToListStatus.put(parentPath, new LocatedFileStatus[] {fileStatus});

        // ensure requests for file status return the file
        pathToFileStatus.put(path, fileStatus);
    }

    private Path createAncestorDirectories(Path path) {
        Path parentPath = null;
        String cumulativePath = scheme + ":";
        String[] parts = path.toUri().getPath().split("/");
        for (String part : Arrays.copyOf(parts, parts.length - 1)) {
            cumulativePath = cumulativePath + part + "/";
            Path childPath = new Path(cumulativePath);
            LocatedFileStatus dir = stubDirectory(new Path(cumulativePath));
            if (null != parentPath) {
                pathToFileStatus.put(childPath, dir);
                pathToListStatus.put(parentPath, new LocatedFileStatus[] {dir});
            }
            parentPath = childPath;
        }
        return parentPath;
    }

    private LocatedFileStatus stubDirectory(Path path) {
        return new LocatedFileStatus(FILE_LENGTH, true, 0, BLOCK_SIZE, MODIFICATION_TIME, 0, null, null, null, null, path, false, false, false, null);
    }

    public static LocatedFileStatus stubFileStatus(Path path) {
        return new LocatedFileStatus(FILE_LENGTH, false, 0, BLOCK_SIZE, MODIFICATION_TIME, 0, null, null, null, null, path, false, false, false, null);
    }

    @Override
    public void setWorkingDirectory(Path newDir) {
        this.workingDirectory = makeAbsolute(newDir);
        checkPath(this.workingDirectory);
    }

    /** Copied from RawLocalFileSystem */
    private Path makeAbsolute(Path f) {
        if (f.isAbsolute()) {
            return f;
        } else {
            return new Path(this.workingDirectory, f);
        }
    }

    @Override
    public Path getWorkingDirectory() {
        return this.workingDirectory;
    }

    @Override
    public FileStatus[] globStatus(Path pathPattern) throws IOException {
        return super.globStatus(pathPattern);
    }

    @Override
    public FileStatus getFileStatus(Path f) {
        return this.pathToFileStatus.get(f);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        // return contents of directory
        FileStatus[] result = this.pathToListStatus.get(f);
        if (null == result) {
            throw new FileNotFoundException("File " + f + " does not exist");
        }
        return result;
    }

    public RemoteIterator<LocatedFileStatus> listFiles(final Path f, final boolean recursive) {
        return new InMemoryRemoteIterator<LocatedFileStatus>(f, recursive);
    }

    @Override
    public URI getUri() {
        return this.uri;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize,
                    Progressable progress) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        throw new UnsupportedOperationException();
    }

    private class InMemoryRemoteIterator<T> implements RemoteIterator<LocatedFileStatus> {
        private final Iterator<LocatedFileStatus> contents;
        private final boolean recursive;

        public InMemoryRemoteIterator(Path f, boolean recursive) {
            this.recursive = recursive;
            LocatedFileStatus fileStatus = pathToFileStatus.get(f);
            contents = getListOfFiles(fileStatus).iterator();
        }

        private List<LocatedFileStatus> getListOfFiles(LocatedFileStatus fileStatus) {
            if (fileStatus == null) {
                return new ArrayList<>();
            }
            if (fileStatus.isFile()) {
                return Collections.singletonList(fileStatus);
            } else if (!recursive) {
                List<LocatedFileStatus> children = Arrays.asList(pathToListStatus.get(fileStatus));
                return children.stream().filter(FileStatus::isFile).collect(Collectors.toList());
            } else {
                List<LocatedFileStatus> results = new ArrayList<>();
                for (LocatedFileStatus child : pathToListStatus.get(fileStatus.getPath())) {
                    results.addAll(getListOfFiles(child));
                }
                return results;
            }
        }

        @Override
        public boolean hasNext() throws IOException {
            return contents.hasNext();
        }

        @Override
        public LocatedFileStatus next() throws IOException {
            return contents.next();
        }
    }
}
