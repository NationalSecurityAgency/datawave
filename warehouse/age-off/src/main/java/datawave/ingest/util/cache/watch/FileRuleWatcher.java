package datawave.ingest.util.cache.watch;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import datawave.iterators.filter.ageoff.FilterRule;

/**
 * File Rule Watch
 */
public class FileRuleWatcher extends FileSystemWatcher<Collection<FilterRule>> {

    private static final Logger log = LogManager.getLogger(FileRuleWatcher.class);

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
            AgeOffRuleLoader ruleLoader = new AgeOffRuleLoader(new FileLoaderDependencyProvider(fs, filePath, iterEnv));
            return ruleLoader.load(in);
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
