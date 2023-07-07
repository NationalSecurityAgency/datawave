package datawave.ingest.table.aggregator;

/**
 * This is similar to {@link KeepCountOnlyUidAggregator} except it never preserves the uid list.
 * <p>
 * Practically this version does not require additional configuration.
 */
public class KeepCountOnlyNoUidAggregator extends KeepCountOnlyUidAggregator {

    public KeepCountOnlyNoUidAggregator() {
        super(0);
    }
}
