package datawave.ingest.util.cache.watch;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.w3c.dom.Node;

public class FileLoaderDependencyProvider implements AgeOffFileLoaderDependencyProvider {
    private final FileSystem fs;
    private final Path filePath;

    public FileLoaderDependencyProvider(FileSystem fs, Path filePath) {
        this.fs = fs;
        this.filePath = filePath;
    }

    @Override
    public InputStream getParentStream(Node parent) throws IOException {

        String parentPathStr = parent.getTextContent();

        if (null == parentPathStr || parentPathStr.isEmpty()) {
            throw new IllegalArgumentException("Invalid parent config path, none specified!");
        }
        // loading parent relative to dir that child is in.
        Path parentPath = new Path(filePath.getParent(), parentPathStr);
        if (!fs.exists(parentPath)) {
            throw new IllegalArgumentException("Invalid parent config path specified, " + parentPathStr + " does not exist!");
        }
        return fs.open(parentPath);
    }
}
