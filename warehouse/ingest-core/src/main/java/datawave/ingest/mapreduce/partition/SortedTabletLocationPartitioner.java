package datawave.ingest.mapreduce.partition;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.TreeMultimap;

import datawave.ingest.mapreduce.job.SplitsCache;
import datawave.ingest.mapreduce.job.SplitsCacheFactory;

public class SortedTabletLocationPartitioner extends MultiTableRangePartitioner {

    private static final Logger log = Logger.getLogger(SortedTabletLocationPartitioner.class);
    private final Map<String,Map<Integer,Integer>> SPLIT_TO_REDUCER_MAP = new HashMap<>();
    private SplitsCache splitsFile;

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
        splitsFile = SplitsCacheFactory.getSplitsCache(getConf());
    }

    @Override
    protected int calculateIndex(int index, int numPartitions, String tableName, int cutPointArrayLength) {
        if (isAssignedPartition(tableName, index)) {
            return SPLIT_TO_REDUCER_MAP.get(tableName).get(index);
        }
        try {
            assignPartitions(numPartitions, tableName, cutPointArrayLength);
        } catch (IOException e) {
            log.error("Unable to assign partitions for " + tableName + ". Defaulting to parent assignment.");
            super.calculateIndex(index, numPartitions, tableName, cutPointArrayLength);

        }
        return isAssignedPartition(tableName, index) ? SPLIT_TO_REDUCER_MAP.get(tableName).get(index) : 0;

    }

    private void assignPartitions(int numPartitions, String tableName, int cutPointArrayLength) throws IOException {
        List<Text> splitsByTable = splitsFile.getSplits(tableName);

        Map<Text,String> currentTableSplitToLocation = splitsFile.getSplitsAndLocationByTable(tableName);
        Map<Integer,Integer> tempSplitReducerMap = new HashMap<>();
        Text[] cutPointArray = splitsByTable.toArray(new Text[0]);

        if (cutPointArrayLength > numPartitions) {
            mapPartitions(numPartitions, cutPointArrayLength, currentTableSplitToLocation, tempSplitReducerMap, cutPointArray);
        } else {
            for (int i = 0; i < cutPointArrayLength; i++) {
                tempSplitReducerMap.put(i, i);
                tempSplitReducerMap.put(-i - 1, i);
            }
        }

        SPLIT_TO_REDUCER_MAP.put(tableName, tempSplitReducerMap);
    }

    private void mapPartitions(int numPartitions, int cutPointArrayLength, Map<Text,String> currentTableSplitToLocation,
                    Map<Integer,Integer> tempSplitReducerMap, Text[] cutPointArray) {

        int locationsAssigned = 0;
        int assignedReducer = 0;

        Map<Integer,Integer> reducerToSplitCount = new HashMap<>();

        TreeMultimap<String,Integer> locationToSplits = TreeMultimap.create();

        for (int k = 0; k < cutPointArrayLength; k++) {
            locationToSplits.put(currentTableSplitToLocation.get(cutPointArray[k]), k);
        }

        Iterator<String> locationIterator = locationToSplits.keySet().iterator();
        while (locationIterator.hasNext()) {
            Set<Integer> splitsForCurrentLocation = locationToSplits.get(locationIterator.next());

            for (Integer splitIndex : splitsForCurrentLocation) {
                tempSplitReducerMap.put(splitIndex, assignedReducer);
                tempSplitReducerMap.put(-splitIndex - 1, assignedReducer);
            }

            locationsAssigned++;
            // simple round robin for now until we've assigned something to each partition
            int sum = null == reducerToSplitCount.get(assignedReducer) ? 0 : reducerToSplitCount.get(assignedReducer);
            reducerToSplitCount.put(assignedReducer, sum + splitsForCurrentLocation.size());

            if (reducerToSplitCount.size() < numPartitions) {
                assignedReducer = locationsAssigned % numPartitions;
            } else {
                // Once all partitions have at least one assignment, look for the one with the smallest number of splits
                int leastSplits = Integer.MAX_VALUE;
                int leastReducer = 0;
                for (Map.Entry<Integer,Integer> reducer : reducerToSplitCount.entrySet()) {
                    if (reducer.getValue() < leastSplits) {
                        leastReducer = reducer.getKey();
                        leastSplits = reducer.getValue();
                    }
                }
                assignedReducer = leastReducer;
            }

        }

    }

    private boolean isAssignedPartition(String tableName, int index) {
        return SPLIT_TO_REDUCER_MAP.containsKey(tableName) && SPLIT_TO_REDUCER_MAP.get(tableName).containsKey(index);
    }

    @Override
    public boolean needSplitLocations() {
        return true;
    }
}
