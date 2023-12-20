package datawave.query.jexl.functions;

import static datawave.query.tld.TLD.parseDatatypeAndRootUidFromEvent;

import org.apache.accumulo.core.data.ByteSequence;

import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.util.TypeMetadata;

public class TLDEventFieldAggregator extends EventFieldAggregator {
    public TLDEventFieldAggregator(String field, EventDataQueryFilter filter, int maxNextCount, TypeMetadata typeMetadata, String defaultTypeClass) {
        super(field, filter, maxNextCount, typeMetadata, defaultTypeClass);
    }

    /**
     * Event Data Key Structure (row, cf='datatype\0uid', cq='field\0value')
     *
     * @param cf
     *            - a ByteSequence representing the Key's ColumnFamily
     */
    @Override
    protected ByteSequence parsePointer(ByteSequence cf) {
        return parseDatatypeAndRootUidFromEvent(cf);
    }
}
