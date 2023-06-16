package datawave.ingest.mapreduce.job;

import datawave.ingest.mapreduce.partition.DelegatePartitioner;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

/**
 * Map containing Text table names as its keys and partitioner offsets at its values. Its a list of table names The offsets are used to
 */
public class TablePartitionerOffsets extends HashMap<Text,Integer> {

    private final Configuration conf;
    private final PartitionerCache partitionerCache;
    private final int reduceTasks;

    public TablePartitionerOffsets(Configuration conf, List<Text> tableNames, PartitionerCache partitionerCache) throws ClassNotFoundException {
        super();
        this.conf = conf;
        this.reduceTasks = conf.getInt("splits.num.reduce", 1);
        this.partitionerCache = partitionerCache;
        TreeMap<Text,Integer> maxPartitionsByTable = getMaxNumPartitionsPerTable(tableNames);
        registerOffsets(tableNames, maxPartitionsByTable);
    }

    private TreeMap<Text,Integer> getMaxNumPartitionsPerTable(List<Text> tableNames) throws ClassNotFoundException {
        TreeMap<Text,Integer> maxPartitionsByTable = new TreeMap();
        for (Text tableName : tableNames) {
            Partitioner<BulkIngestKey,Value> partitioner = partitionerCache.getPartitioner(tableName);
            if (partitioner instanceof DelegatePartitioner) {
                maxPartitionsByTable.put(tableName, ((DelegatePartitioner) partitioner).getNumPartitions());
            } else {
                maxPartitionsByTable.put(tableName, Integer.MAX_VALUE);
            }
        }
        return maxPartitionsByTable;
    }

    private void registerOffsets(List<Text> tableNames, TreeMap<Text,Integer> maxPartitionsByTable) {
        HashMap<Text,Integer> offsetsByCategoryName = new HashMap<>();
        // start the offsets from the back
        int previousOffsetStart = this.reduceTasks;
        for (Text tableName : tableNames) {
            previousOffsetStart = registerOffsetForTable(maxPartitionsByTable, offsetsByCategoryName, previousOffsetStart, tableName);
        }
    }

    private int registerOffsetForTable(TreeMap<Text,Integer> maxPartitionsByTable, HashMap<Text,Integer> offsetsByCategoryName, int previousOffsetStart,
                    Text tableName) {
        int numPartitionsNeededForTable = maxPartitionsByTable.get(tableName);
        Text categoryName = partitionerCache.getCategory(conf, tableName);
        int additionalPartitionSlotsNeeded = determineHowManySlotsToAdd(offsetsByCategoryName, categoryName, numPartitionsNeededForTable);
        int offsetForThisTable = getOffsetForTable(offsetsByCategoryName, categoryName, numPartitionsNeededForTable, previousOffsetStart);
        this.put(tableName, offsetForThisTable);
        return previousOffsetStart - additionalPartitionSlotsNeeded;
    }

    // dont use slots if the partitioner uses the entire reducer space or the category offset was already determined
    private int determineHowManySlotsToAdd(HashMap<Text,Integer> offsetsByCategoryName, Text categoryName, int partitionsForTableName) {
        int additionalPartitionSlotsNeeded = partitionsForTableName;
        if (additionalPartitionSlotsNeeded == Integer.MAX_VALUE || (null != categoryName && null != offsetsByCategoryName.get(categoryName))) {
            additionalPartitionSlotsNeeded = 0;
        }
        return additionalPartitionSlotsNeeded;
    }

    private int getOffsetForTable(HashMap<Text,Integer> offsetsByCategoryName, Text categoryName, Integer numPartitionsNeededForTable, int offset) {
        if (numPartitionsNeededForTable == Integer.MAX_VALUE) {
            return 0;
        } else {
            int offsetStart = (offset - numPartitionsNeededForTable);
            while (offsetStart < 0) {
                offsetStart = (offsetStart + reduceTasks) % reduceTasks;
            }
            if (null != categoryName) {
                if (null == offsetsByCategoryName.get(categoryName)) {
                    // save for other category members
                    offsetsByCategoryName.put(categoryName, offsetStart);
                } else {
                    // if category offset was already determined, use it
                    return offsetsByCategoryName.get(categoryName);
                }
            }
            return offsetStart;
        }
    }
}
