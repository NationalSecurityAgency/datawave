package datawave.ingest.mapreduce.partition;

import com.google.common.collect.TreeMultimap;
import datawave.ingest.mapreduce.job.SplitsFileType;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

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
        log.trace("Index was not found after recomputing the reducer map");
        return 0;
    }

    private void assignPartitions(int numPartitions, String tableName, int cutPointArrayLength) {
        Map<Text, String> currentTableSplitToLocation = splitToLocationMap.get().get(tableName);
        Map<Integer,Integer> tempSplitReducerMap = new HashMap<>();
        Text[] cutPointArray = splitsByTable.get().get(tableName);

        if (cutPointArrayLength > numPartitions) {
            mapPartitions(numPartitions, cutPointArrayLength, currentTableSplitToLocation, tempSplitReducerMap, cutPointArray);
        } else
            for (int i = 0; i < cutPointArrayLength; i++){
            tempSplitReducerMap.put(i, i);
            tempSplitReducerMap.put(-i - 1, i);
            }

            SPLIT_TO_REDUCER_MAP.put(tableName, tempSplitReducerMap);
    }

    private void mapPartitions(int numPartitions, int cutPointArrayLength, Map<Text, String> currentTableSplitToLocation, Map<Integer,Integer> tempSplitReducerMap, Text[] cutPointArray) {
        int locationsPerSmallReducer;
        int numReducersWithExtraLocation;
        int firstLargeReducer;
        int locationsAssigned = 0;
        int assignedReducer = 0;
        int numLocations;
        boolean reducerFull;
        TreeMultimap<String, Integer> locationToSplits = TreeMultimap.create();

        for (int k = 0; k < cutPointArrayLength; k++) {
            locationToSplits.put(currentTableSplitToLocation.get(cutPointArray[k]), k);
        }

        numLocations = locationToSplits.keys().size();
        locationsPerSmallReducer = numLocations / numPartitions;
        numReducersWithExtraLocation = numLocations % numPartitions;
        firstLargeReducer = numPartitions - numReducersWithExtraLocation;

        Iterator<String> locationIterator = locationToSplits.keySet().iterator();
        while (locationIterator.hasNext()) {
            Set<Integer> splitsForCurrentLocation = locationToSplits.get(locationIterator.next());

            for (Integer splitIndex : splitsForCurrentLocation) {
                tempSplitReducerMap.put(splitIndex, assignedReducer);
                tempSplitReducerMap.put(-splitIndex - 1, assignedReducer);
            }
            locationsAssigned++;
            reducerFull = ((locationsAssigned == locationsPerSmallReducer) && (assignedReducer < firstLargeReducer)) || ((locationsAssigned == locationsPerSmallReducer + 1) && (assignedReducer >= firstLargeReducer));
            if (reducerFull)
                assignedReducer = incrementReducer(numPartitions, assignedReducer);
        }

        }

        private int incrementReducer(int numPartitions, int i){
        if(i < numPartitions - 1){
            i++;

        }else
            i=0;

        return i;
        }

        private boolean isAssignedPartition(String tableName, int index){
            return SPLIT_TO_REDUCER_MAP.containsKey(tableName) && SPLIT_TO_REDUCER_MAP.get(tableName).containsKey(index);
        }

        @Override
        protected SplitsFileType getSplitsFileType() {
            return SplitsFileType.SPLITSANDLOCATIONS;
        }

    }


