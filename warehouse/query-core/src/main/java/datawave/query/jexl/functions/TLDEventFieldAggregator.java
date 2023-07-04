package datawave.query.jexl.functions;

import java.util.ArrayList;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;

import datawave.data.hash.UID;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.tld.TLD;
import datawave.query.util.TypeMetadata;

public class TLDEventFieldAggregator extends EventFieldAggregator {
    public TLDEventFieldAggregator(String field, EventDataQueryFilter filter, int maxNextCount, TypeMetadata typeMetadata, String defaultTypeClass) {
        super(field, filter, maxNextCount, typeMetadata, defaultTypeClass);
    }

    @Override
    protected ByteSequence parsePointer(ByteSequence columnFamily) {
        // find the null between the dataType and Uid
        ArrayList<Integer> nulls = TLD.instancesOf(0, columnFamily, 1);
        final int start = nulls.get(0) + 1;
        // uid is from the null byte to the end of the cf
        ByteSequence uidByteSequence = columnFamily.subSequence(start, columnFamily.length());

        UID uid = UID.parseBase(uidByteSequence.toString());
        if (uid.getBaseUid().length() != uidByteSequence.length()) {
            return new ArrayByteSequence(columnFamily.subSequence(0, start) + uid.getBaseUid());
        }

        // no reduction necessary, already tld
        return columnFamily;
    }
}
