package datawave.ingest.mapreduce.partition;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Logger;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.util.StringUtils;

/**
 * Attempts to distribute rows among reducers evenly. This can also be configured to also shuffle specific column families within each row
 *
 * The base functionality of this partitioner is to have different shards distributed among the reducers in round robin fashion.
 */
public class RowHashingPartitioner extends Partitioner<BulkIngestKey,Value> implements DelegatePartitioner {
    public static final String COLUMN_FAMILIES = "datawave.partitioner.rr.colfams";

    private static Logger log = Logger.getLogger(RowHashingPartitioner.class);

    private Configuration conf;
    private Set<Text> colFams = Collections.emptySet();

    @Override
    public int getPartition(BulkIngestKey bKey, Value value, int reducers) {
        HashCodeBuilder hcb = new HashCodeBuilder(157, 41);
        Key key = bKey.getKey();
        Text cf = key.getColumnFamily();
        if (colFams.contains(cf)) {
            hcb.append(cf);
        }
        hcb.append(key.getRow());
        int partition = (hcb.toHashCode() >>> 1) % reducers;
        if (log.isTraceEnabled()) {
            log.trace("Returning " + partition + " for BIK " + bKey);
        }
        return partition;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        configure(COLUMN_FAMILIES);
    }

    private void configure(String propertyName) {
        // build the colFams set
        String columnFamiliesCsv = conf.get(propertyName, "");
        if (!columnFamiliesCsv.isEmpty()) {
            String[] cfs = StringUtils.split(columnFamiliesCsv, ',');
            colFams = new HashSet<>();
            for (String cf : cfs) {
                colFams.add(new Text(cf));
            }
        }
    }

    @Override
    public void configureWithPrefix(String prefix) {
        configure(prefix + '.' + COLUMN_FAMILIES);
    }

    @Override
    public int getNumPartitions() {
        return Integer.MAX_VALUE;
    }

    @Override
    public void initializeJob(Job job) {
        // no op
    }

    @Override
    public boolean needSplits() {
        return false;
    }

    @Override
    public boolean needSplitLocations() {
        return false;
    }
}
