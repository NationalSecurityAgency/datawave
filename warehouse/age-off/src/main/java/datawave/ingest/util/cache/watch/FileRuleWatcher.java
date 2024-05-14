package datawave.ingest.util.cache.watch;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;
import org.w3c.dom.Node;

import datawave.iterators.filter.ageoff.FilterRule;

/**
 * File Rule Watch
 */
public class FileRuleWatcher extends FileSystemWatcher<Collection<FilterRule>> {

    private static final Logger log = Logger.getLogger(FileRuleWatcher.class);

    private final IteratorEnvironment iterEnv;

    /**
     * @param fs
     *            file system
     * @param filePath
     *            path to the file
     * @param configuredDiff
     *            configured diff
     * @throws IOException
     *             if there is a problem reading the file
     */
    public FileRuleWatcher(FileSystem fs, Path filePath, long configuredDiff) throws IOException {
        this(fs, filePath, configuredDiff, null);
    }

    /**
     * @param fs
     *            file system
     * @param filePath
     *            path to the file
     * @param configuredDiff
     *            configured diff
     * @param iterEnv
     *            iterator environment
     * @throws IOException
     *             if there is a problem reading the file
     */
    public FileRuleWatcher(FileSystem fs, Path filePath, long configuredDiff, IteratorEnvironment iterEnv) throws IOException {
        super(fs, filePath, configuredDiff);
        this.iterEnv = iterEnv;
    }

    /**
     * @param filePath
     *            path to the file
     * @param configuredDiff
     *            configured diff
     * @throws IOException
     *             if there is an error reading the file
     */
    public FileRuleWatcher(Path filePath, long configuredDiff) throws IOException {
        this(filePath, configuredDiff, null);
    }

    /**
     * @param filePath
     *            the path to the file
     * @param configuredDiff
     *            configured diff
     * @param iterEnv
     *            iterator environment
     * @throws IOException
     *             if there is an error reading the file
     */
    public FileRuleWatcher(Path filePath, long configuredDiff, IteratorEnvironment iterEnv) throws IOException {
        super(filePath.getFileSystem(new Configuration()), filePath, configuredDiff);
        this.iterEnv = iterEnv;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.ingest.util.cache.watch.FileSystemWatcher#loadContents(java.io.InputStream)
     */
    @Override
    protected Collection<FilterRule> loadContents(InputStream in) throws IOException {
        try {
            AgeOffRuleLoader ruleLoader = new AgeOffRuleLoader(new FileWatcherDependencyProvider());
            return ruleLoader.load(in);
        } catch (Exception ex) {
            log.error("uh oh: " + ex);
            throw new IOException(ex);
        } finally {
            IOUtils.closeStream(in);
        }
    }

    private class FileWatcherDependencyProvider implements AgeOffRuleLoader.AgeOffFileLoaderDependencyProvider {
        @Override
        public IteratorEnvironment getIterEnv() {
            return iterEnv;
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

}
