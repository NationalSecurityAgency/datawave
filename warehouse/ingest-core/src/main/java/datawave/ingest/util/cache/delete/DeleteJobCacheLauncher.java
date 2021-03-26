package datawave.ingest.util.cache.delete;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import datawave.ingest.util.ConfigurationFileHelper;
import datawave.ingest.util.cache.converter.DeleteModeConverter;
import datawave.ingest.util.cache.delete.mode.DeleteJobCacheMode;
import datawave.ingest.util.cache.delete.mode.DeleteModeOptions;
import datawave.ingest.util.cache.delete.mode.OldInactiveMode;
import datawave.ingest.util.cache.lease.JobCacheLockFactory;
import datawave.ingest.util.cache.lease.LockFactory;
import datawave.ingest.util.cache.path.FileSystemPath;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Delete job cache launcher will delete specified or old/inactive job cache directories.
 */

public class DeleteJobCacheLauncher {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteJobCacheLauncher.class);
    
    @Parameter(names = {"--delete-mode"}, description = "Mode to determine how to delete job cache directories.", converter = DeleteModeConverter.class)
    DeleteJobCacheMode deleteMode = new OldInactiveMode();
    
    @Parameter(names = {"--hadoop-conf-dirs"}, description = "The hadoop configuration directories")
    List<String> hadoopConfDirs = Arrays.asList(System.getenv("INGEST_HADOOP_CONF"), System.getenv("WAREHOUSE_HADOOP_CONF"));
    
    @Parameter(names = {"-h", "-help", "--help", "-?"}, help = true, description = "Prints job options.")
    boolean help;
    
    @Parameter(names = {"--cache-namespace"}, description = "Zookeeper namespace to check for active jobs")
    String cacheNamespace = "datawave/jobCache/";
    
    @Parameter(names = {"--lock-timeout-in-ms"}, description = "Zookeeper timeout for acquiring lock")
    int lockTimeoutInMs = 30;
    
    @Parameter(names = {"--lock-retry-timeout-in-ms"}, description = "Zookeeper timeout for retrying connection ")
    int lockRetryTimeoutInMs = 30;
    
    @Parameter(names = {"--lock-retry-count"}, description = "Zookeeper retry connection attempts ")
    int lockRetryCount = 30;
    
    @Parameter(names = {"--zookeepers"}, description = "Zookeeper instances.", required = true)
    String zookeepers;
    
    /**
     * Will delete job cache paths based on the deletion mode specified.
     *
     * @param options
     *            Mode options that will determine which job caches to delete.
     */
    public void run(DeleteModeOptions options) {
        Collection<Configuration> hadoopConfs = ConfigurationFileHelper.getHadoopConfs(hadoopConfDirs);
        Collection<FileSystemPath> deletionCandidates = deleteMode.getDeletionCandidates(hadoopConfs, options);
        
        try (LockFactory lockFactory = new JobCacheLockFactory(cacheNamespace, zookeepers, lockTimeoutInMs, lockRetryCount, lockRetryTimeoutInMs)) {
            LOGGER.info("Attempting to delete inactive caches {}", listToString(deletionCandidates));
            DeleteJobCache.deleteCacheIfNotActive(deletionCandidates, lockFactory);
        } catch (Exception e) {
            LOGGER.error("Unable to delete caches {} ", listToString(deletionCandidates), e);
        } finally {
            deletionCandidates.forEach(FileSystemPath::close);
        }
    }
    
    /**
     * Will transform a collection of objects to single string representation for logging.
     *
     * @param collection
     *            Collection of objects to transform to a string.
     * @return A string representing the collection separating with a comma delimiter.
     */
    public static String listToString(Collection<?> collection) {
        // @formatter:off
        return collection
                .stream()
                .map(Object::toString)
                .collect(Collectors.joining(","));
        // @formatter:on
    }
    
    public static void main(String[] args) throws Exception {
        LOGGER.info("Starting delete job cache utility");
        DeleteJobCacheLauncher launcher = new DeleteJobCacheLauncher();
        DeleteModeOptions options = new DeleteModeOptions();
        
        // @formatter:off
        JCommander jCommander = JCommander
                .newBuilder()
                .addObject(launcher)
                .addObject(options)
                .build();
        // @formatter:on
        
        try {
            jCommander.parse(args);
            if (launcher.help) {
                jCommander.usage();
            } else {
                launcher.run(options);
            }
        } catch (ParameterException e) {
            System.err.println(e.getMessage());
            jCommander.usage();
            System.exit(-1);
        }
        LOGGER.info("Finished delete job cache utility");
    }
}
