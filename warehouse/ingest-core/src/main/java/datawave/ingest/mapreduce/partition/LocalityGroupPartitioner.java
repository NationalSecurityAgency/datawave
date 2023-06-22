package datawave.ingest.mapreduce.partition;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.util.StringUtils;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Logger;

import java.util.ArrayList;

/**
 * Directs each locality group to its own partition. Puts anything else in another bin (assumes that every column family is named and evenly distributed)
 * Designed for experimentation with the dqr hourly index in mind
 *
 */
public class LocalityGroupPartitioner extends Partitioner<BulkIngestKey,Value> implements DelegatePartitioner {
    private static Logger log = Logger.getLogger(LocalityGroupPartitioner.class);

    public static final String COLUMN_FAMILIES = "LocalityGroupPartitioner.colfams"; // csv

    private Configuration conf;
    private ArrayList<Text> colFams = new ArrayList<>();
    private boolean hasSeenUnknownColFams = false;

    @Override
    public int getPartition(BulkIngestKey bKey, Value value, int reducers) {
        Text columnFamily = bKey.getKey().getColumnFamily();
        int index = colFams.indexOf(columnFamily);
        if (index == -1) {
            if (!hasSeenUnknownColFams) {
                log.warn("Unexpected column family: " + columnFamily);
                hasSeenUnknownColFams = true;
            }
            return colFams.size() % reducers; // spill over unrecognized partition
        }
        return index % reducers;
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
            colFams = new ArrayList<>();
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
        return colFams.size() + 1; // +1 because we spill over unrecognized col families into another partition
    }

    @Override
    public void initializeJob(Job job) {
        // no op
    }
}
