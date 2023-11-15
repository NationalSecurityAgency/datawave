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
    private final FlagFileContentCreator flagFileContentCreator;

    public SizeValidatorImpl(Configuration hadoopConfiguration, FlagMakerConfig flagMakerConfig) {
        this.flagFileContentCreator = new FlagFileContentCreator(flagMakerConfig);
        this.hadoopConfiguration = hadoopConfiguration;
        this.maxFileLength = flagMakerConfig.getMaxFileLength();
    }

    @Override
    public boolean isValidSize(FlagDataTypeConfig fc, Collection<InputFile> files) {
        return fitsWithinCounterLimit(fc, files) && expectedFileSizeIsUnderMaximum(fc, files);
    }

    private boolean fitsWithinCounterLimit(FlagDataTypeConfig fc, Collection<InputFile> files) {
        int counterLimit = getCounterLimit(this.hadoopConfiguration);
        int expectedNumberOfCounters = (files.size() * COUNTERS_PER_INPUT_FILE) + COUNTERS_PER_FLAG_FILE;

        if (expectedNumberOfCounters > counterLimit) {
            int allowedNumInputFiles = (counterLimit - COUNTERS_PER_FLAG_FILE) / COUNTERS_PER_INPUT_FILE;
            LOG.warn("Check hadoop configuration. Counter limit ({}) exceeded for {}. Restricting to {} input files per flag file.", counterLimit,
                            fc.getDataName(), allowedNumInputFiles);
            return false;
        }
        return true;
    }

    private boolean expectedFileSizeIsUnderMaximum(FlagDataTypeConfig fc, Collection<InputFile> files) {
        long expectedFileSize = calculateFlagFileSize(fc, files);

        if (expectedFileSize > maxFileLength) {
            LOG.warn("Flag file size for {} exceeding {}.  Reducing number of input files to compensate", fc.getDataName(), maxFileLength);
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
     * @param flagDataTypeConfig
     *            configuration for datatype
     * @param inFiles
     *            collection of input files to include in flag file
     * @return Expected size in characters of the flag file
     */
    long calculateFlagFileSize(FlagDataTypeConfig flagDataTypeConfig, Collection<InputFile> inFiles) {
        int flagFileSize = flagFileContentCreator.calculateSize(inFiles, flagDataTypeConfig);
        LOG.debug("Calculated flag file size: " + flagFileSize);
        return flagFileSize;
    }
}
