package datawave.ingest.util.cache.watch;

import com.google.common.annotations.VisibleForTesting;
import datawave.iterators.filter.ageoff.AppliedRule;
import datawave.iterators.filter.ageoff.FilterRule;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

/**
 * Rule cache value implementation for use with age-off rule loading. The implementation is thread-safe and supports concurrent access for all methods.
 */
public class FileRuleCacheValue {
    private final static Logger log = Logger.getLogger(FileRuleCacheValue.class);

    private final Path filePath;
    private final long configuredDiff;
    private final FileSystem fs;

    private volatile FileRuleReference ruleRef;

    FileRuleCacheValue(FileSystem fs, Path filePath, long configuredDiff) {
        this.filePath = filePath;
        this.configuredDiff = configuredDiff;
        this.fs = fs;
    }

    /**
     * Creates a new instance of this class for the specified @param filePath. Actual evaluation of the @param filePath are deferred until calls to
     * {@link #newRulesetView(long, IteratorEnvironment)}
     *
     * @param filePath
     *            the file path to prepare a cached representation on
     * @param configuredDiff
     *            the threshold time (in milliseconds) for when timestamp differences are considered changes
     * @return a new cache value instance
     * @throws IOException
     *             if the cache value instance cannot be created
     */
    public static FileRuleCacheValue newCacheValue(String filePath, long configuredDiff) throws IOException {
        Path filePathObj = new Path(filePath);
        FileSystem fs = filePathObj.getFileSystem(new Configuration());
        return new FileRuleCacheValue(fs, filePathObj, configuredDiff);
    }

    /**
     * Gets the file path of this instance.
     *
     * @return path for the instance
     */
    public Path getFilePath() {
        return filePath;
    }

    /**
     * Check if the cached representation has changes. Changes are determined by checking the baseline modification time when the cached representation was
     * discovered against the current modification time of the file.
     *
     * @return true if there are changes, otherwise false
     */
    public boolean hasChanges() {
        if (ruleRef == null) {
            return true;
        }
        long currentTime;
        try {
            currentTime = fs.getFileStatus(filePath).getModificationTime();
        } catch (IOException e) {
            log.debug("Error getting file status for: " + filePath, e);
            return true;
        }
        long previousTime = ruleRef.getTimestamp();
        boolean changed = (currentTime - previousTime) > configuredDiff;
        if (log.isTraceEnabled()) {
            log.trace("Changes result: " + changed + ", current time: " + currentTime);
        }
        return changed;
    }

    /**
     * Creates a new ruleset view of the file. The initial call to the method will lazily create the base rules and return a view of the baseline rules. The
     * next calls will create new view copies derived from the baseline rules.
     *
     * @param scanStart
     *            the start of a scan operation to use for the ruleset
     * @param iterEnv
     *            the iterator environment for the scan
     * @return a deep copy of the cached {@link AppliedRule} baseline rules
     * @throws IOException
     *             if there are errors during the cache value creation, on initial call
     */
    public Collection<AppliedRule> newRulesetView(long scanStart, IteratorEnvironment iterEnv) throws IOException {
        // rule initialization/copies are performed on the calling thread
        // the base iterator rules will use an iterator environment from the caller (and keep in the AppliedRule)
        // the deep copy always creates new views of the rules with the caller's iterator environment
        if (ruleRef == null) {
            long ts = fs.getFileStatus(filePath).getModificationTime();
            Collection<FilterRule> rulesBase = loadFilterRules(iterEnv);
            ruleRef = new FileRuleReference(ts, rulesBase);
        }
        return ruleRef.deepCopy(scanStart, iterEnv);
    }

    @VisibleForTesting
    Collection<FilterRule> loadFilterRules(IteratorEnvironment iterEnv) throws IOException {
        AgeOffRuleLoader ruleLoader = new AgeOffRuleLoader(new FileLoaderDependencyProvider(fs, filePath, iterEnv));
        Collection<FilterRule> rulesBase;
        try (InputStream in = fs.open(filePath)) {
            rulesBase = ruleLoader.load(in);
        } catch (Exception e) {
            // all exceptions are wrapped in an IOException
            log.error("Error loading file: " + filePath, e);
            throw new IOException(e);
        }
        return rulesBase;
    }

    @VisibleForTesting
    FileRuleReference getRuleRef() {
        return ruleRef;
    }
}
