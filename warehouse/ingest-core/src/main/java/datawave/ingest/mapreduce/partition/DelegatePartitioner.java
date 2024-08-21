package datawave.ingest.mapreduce.partition;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.mapreduce.Job;

// See DelegatingPartitioner
// There could be multiple instances of a partitioner, for different tables.
// Each may have a different setting.
// most of these are optional implementations except for get num partitions
public interface DelegatePartitioner extends Configurable {
    /**
     * Assumes that setConf has already been called and the conf saved off
     *
     * @param prefix
     *            a prefix to add for the property names that the partitioner needs
     */
    void configureWithPrefix(String prefix);

    /**
     * Delegates can use a smaller number of partitions to avoid creating too many small rfiles If there are multiple partitioners like this, it is best to
     * offset them, e.g. not to send them all to reducer 0.
     *
     * @return max num partitions this partitioner will return, Integer.MAX_VALUE if it doesn't limit it
     */
    int getNumPartitions();

    /**
     * Will likely do nothing for most partitioners. This should only be used for operations that are done during job initialization. e.g., a relatively
     * expensive load of the key extends from accumulo are put into a file and then saved to the job cache. If required, this should be smart enough to run
     * exactly once. So it should check for the existance of a property that indicates it does not need to recreate the file.
     *
     * @param job
     *            job to be configured
     */
    void initializeJob(Job job);

    boolean needSplits();

    boolean needSplitLocations();
}
