package datawave.ingest.mapreduce.partition;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class MultiTableRRRangePartitioner extends MultiTableRangePartitioner {

    private static final Logger LOG = Logger.getLogger(MultiTableRRRangePartitioner.class);
    private final Map<String,Map<Integer,Integer>> SPLIT_TO_REDUCER_MAP = new HashMap();

    @Override
    protected int calculateIndex(int index, int numPartitions, String tableName, int cutPointArrayLength) {
        // check to see if the index is already in the SPLIT_TO_REDUCER_MAP map, if so, return the reducer number
        if (SPLIT_TO_REDUCER_MAP.containsKey(tableName) && SPLIT_TO_REDUCER_MAP.get(tableName).containsKey(index)) {
            return SPLIT_TO_REDUCER_MAP.get(tableName).get(index);
        } else {
            int i = cutPointArrayLength;
            int reducer = numPartitions - 1;
            Map<Integer,Integer> tempSplitReducerMap = new HashMap<>();
            // start with the index that represents a value greater than all values in the cutPointArray, start filling in the mapping of indices to reducers
            tempSplitReducerMap.put(i, reducer);
            tempSplitReducerMap.put(-i - 1, reducer);
            i--;
            reducer--;
            // continue to map reducers to all possible binary search values for the cutPointArray
            while (i >= 0) {
                while (reducer >= 0) {
                    tempSplitReducerMap.put(i, reducer);
                    tempSplitReducerMap.put(-i - 1, reducer);
                    i--;
                    reducer--;
                    if (i < 0)
                        break;
                }
                reducer = numPartitions - 1;
            }
            SPLIT_TO_REDUCER_MAP.put(tableName, tempSplitReducerMap);
            if (SPLIT_TO_REDUCER_MAP.containsKey(tableName) && SPLIT_TO_REDUCER_MAP.get(tableName).containsKey(index)) {
                return SPLIT_TO_REDUCER_MAP.get(tableName).get(index);
            }
            // if this index is not already assigned a partition, return 0. The 0 partition will always have the least splits unless each partition has an equal
            // amount.
            LOG.trace("Index was not found after recomputing the reducer map");
            return 0;

        }
    }

}
