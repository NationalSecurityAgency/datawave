package nsa.datawave.query.index.lookup;

/**
 * Merge ranges on all IndexInfo to prevent returning uids that are within the same branch as another uid in the IndexInfo
 */
public class AncestorCreateUidsIterator extends CreateUidsIterator {
    @Override
    public IndexInfo getValue() {
        tv = AncestorIndexStream.mergeRanges(tv);
        return super.getValue();
    }
}
