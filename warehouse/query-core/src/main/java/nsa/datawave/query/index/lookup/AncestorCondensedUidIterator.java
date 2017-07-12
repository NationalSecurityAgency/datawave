package nsa.datawave.query.index.lookup;

import java.util.Map;

/**
 * Merge ranges on all IndexInfo to prevent returning uids that are within the same branch as another uid in the IndexInfo
 */
public class AncestorCondensedUidIterator extends CondensedUidIterator {
    @Override
    public CondensedIndexInfo getValue() {
        Map<String,IndexInfo> infoMap = tv.indexInfos;
        for (String key : infoMap.keySet()) {
            IndexInfo info = infoMap.get(key);
            IndexInfo merged = AncestorIndexStream.mergeRanges(info);
            infoMap.put(key, merged);
        }
        
        return super.getValue();
    }
}
