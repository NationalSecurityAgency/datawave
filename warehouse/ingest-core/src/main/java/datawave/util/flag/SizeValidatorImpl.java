package datawave.util.flag;

import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.util.flag.config.FlagDataTypeConfig;
import datawave.util.flag.config.FlagMakerConfig;
import datawave.util.flag.processor.SizeValidator;

/**
 * Extracted from FlagMaker.java. Validates that the flag file contents will not exceed the configured maximum length and also that the expected number of flag
 * file counters will not exceed the hadoop limit
 */
public class SizeValidatorImpl implements SizeValidator {
    private static final Logger LOG = LoggerFactory.getLogger(SizeValidatorImpl.class);

    private static final String COUNTER_LIMIT_HADOOP_2 = "mapreduce.job.counters.max";
    private static final String COUNTER_LIMIT_HADOOP_1 = "mapreduce.job.counters.limit";
    private static final int COUNTERS_PER_INPUT_FILE = 2;
    private static final int COUNTERS_PER_FLAG_FILE = 2;

    private final Configuration hadoopConfiguration;
    private final int maxFileLength;
    private final int datawaveHomeLength;
    private final int flagFileDirectoryLength;
    private final FlagFileContentCreator flagFileContentCreator;

    public SizeValidatorImpl(Configuration hadoopConfiguration, FlagMakerConfig flagMakerConfig) {
        this.flagFileContentCreator = new FlagFileContentCreator(flagMakerConfig);
        this.hadoopConfiguration = hadoopConfiguration;
        this.maxFileLength = flagMakerConfig.getMaxFileLength();
        this.datawaveHomeLength = flagMakerConfig.getDatawaveHome().length();
        this.flagFileDirectoryLength = flagMakerConfig.getFlagFileDirectory().length();
    }

    @Override
    public boolean isValidSize(FlagDataTypeConfig fc, Collection<InputFile> files) {
        return fitsWithinCounterLimit(fc, files) && expectedFileSizeIsUnderMaximum(fc, files);
    }

    private boolean fitsWithinCounterLimit(FlagDataTypeConfig fc, Collection<InputFile> files) {
        int counterLimit = getCounterLimit(this.hadoopConfiguration);
        int expectedNumberOfCounters = (files.size() * COUNTERS_PER_INPUT_FILE) + COUNTERS_PER_FLAG_FILE;

        if (expectedNumberOfCounters > counterLimit) {
            LOG.warn("Check hadoop configuration. Counter limit ({}) exceeded for {}. Restricting to {} files per flag file.", counterLimit, fc.getDataName(),
                            ((counterLimit - COUNTERS_PER_FLAG_FILE) / COUNTERS_PER_INPUT_FILE));
            return false;
        }
        return true;
    }

    private boolean expectedFileSizeIsUnderMaximum(FlagDataTypeConfig fc, Collection<InputFile> files) {
        long expectedFileSize = calculateFlagFileSize(fc, files);

        if (expectedFileSize > maxFileLength) {
            LOG.warn("Flag file size for {} exceeding {}.  Reducing number of files to compensate", fc.getDataName(), maxFileLength);
            return false;
        }

        return true;
    }

    private int getCounterLimit(Configuration configuration) {
        if (configuration.get(COUNTER_LIMIT_HADOOP_2) != null) {
            return Integer.parseInt(configuration.get(COUNTER_LIMIT_HADOOP_2));
        } else if (configuration.get(COUNTER_LIMIT_HADOOP_1) != null) {
            return Integer.parseInt(configuration.get(COUNTER_LIMIT_HADOOP_1));
        } else {
            return Integer.MAX_VALUE;
        }
    }

    /**
     * Get the length of the flag file that would be created using this set of files.
     *
     * @param fc
     *            configuration for datatype
     * @param inFiles
     *            collection of input files to include in flag file
     * @return Expected size in characters of the flag file
     */
    long calculateFlagFileSize(FlagDataTypeConfig fc, Collection<InputFile> inFiles) {
        int fileSize = flagFileContentCreator.calculateSize(inFiles, fc);
        LOG.debug("calculateFlagFileSize: " + fileSize);
        return fileSize;
    }
}
