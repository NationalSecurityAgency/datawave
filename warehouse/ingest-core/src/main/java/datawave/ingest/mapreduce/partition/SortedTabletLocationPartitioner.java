package datawave.ingest.mapreduce.partition;

import com.google.common.collect.TreeMultimap;
import datawave.ingest.mapreduce.job.SplitsFileType;
import org.apache.commons.collections.BinaryHeap;
import org.apache.commons.collections.buffer.PriorityBuffer;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import sun.tools.java.BinaryExceptionHandler;

import java.util.*;

public class SortedTabletLocationPartitioner extends MultiTableRangePartitioner {
    
    private static final Logger log = Logger.getLogger(SortedTabletLocationPartitioner.class);
    private final Map<String,Map<Integer,Integer>> SPLIT_TO_REDUCER_MAP = new HashMap<>();
    
    @Override
    protected int calculateIndex(int index, int numPartitions, String tableName, int cutPointArrayLength) {
        if (isAssignedPartition(tableName, index)) {
            return SPLIT_TO_REDUCER_MAP.get(tableName).get(index);
        }
        assignPartitions(numPartitions, tableName, cutPointArrayLength);
        return isAssignedPartition(tableName, index) ? SPLIT_TO_REDUCER_MAP.get(tableName).get(index) : 0;
        
    }
    
    private void assignPartitions(int numPartitions, String tableName, int cutPointArrayLength) {
        Map<Text,String> currentTableSplitToLocation = splitToLocationMap.get().get(tableName);
        Map<Integer,Integer> tempSplitReducerMap = new HashMap<>();
        Text[] cutPointArray = splitsByTable.get().get(tableName);
        
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
    protected SplitsFileType getSplitsFileType() {
        return SplitsFileType.SPLITSANDLOCATIONS;
    }
    
}
