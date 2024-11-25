package datawave.query.jexl.lookups;

/**
 * An {@link IndexLookup} for a term that previously failed expansion, so will not be looked up again
 */
public class ExceededIndexLookup extends IndexLookup {

    private final IndexLookupMap lookupMap;

    public ExceededIndexLookup() {
        super(null, null);

        // stub out an index lookup that 'exceeded' it's threshold
        lookupMap = new IndexLookupMap(0, 0);
        lookupMap.setKeyThresholdExceeded();
    }

    @Override
    public IndexLookupMap lookup() {
        return lookupMap;
    }
}
