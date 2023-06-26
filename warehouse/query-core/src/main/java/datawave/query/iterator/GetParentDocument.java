package datawave.query.iterator;

import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

import datawave.query.attributes.Document;
import datawave.query.function.Aggregation;
import datawave.query.function.KeyToDocumentData;
import datawave.query.tld.TLD;
import datawave.query.util.EntryToTuple;
import datawave.query.util.Tuple2;

public class GetParentDocument implements Function<Entry<Key,Document>,Tuple2<Key,Document>> {
    private final KeyToDocumentData fetchDocData;
    private final Aggregation makeDocument;
    private final EntryToTuple<Key,Document> convert = new EntryToTuple<>();
    private static final Logger log = Logger.getLogger(GetParentDocument.class);

    public GetParentDocument(KeyToDocumentData fetchDocData, Aggregation makeDocument) {
        this.fetchDocData = fetchDocData;
        this.makeDocument = makeDocument;
    }

    public Tuple2<Key,Document> apply(Entry<Key,Document> from) {
        if (log.isTraceEnabled())
            log.trace("Apply parent key " + from.getKey());
        Key parentKey = TLD.buildParentKey(from.getKey().getRow(), TLD.parseParentPointerFromId(from.getKey().getColumnFamilyData()),
                        from.getKey().getColumnQualifierData(), from.getKey().getColumnVisibility(), from.getKey().getTimestamp());
        if (log.isTraceEnabled())
            log.trace("parent key " + parentKey);
        Entry<Key,Document> parentEntry = Maps.immutableEntry(parentKey, new Document());
        Entry<Key,Document> aggParentEntry = makeDocument.apply(this.fetchDocData.apply(parentEntry));
        Entry<Key,Document> keySwap = Maps.immutableEntry(from.getKey(), aggParentEntry.getValue());
        if (log.isTraceEnabled())
            log.trace("Key Swap is " + keySwap);
        return convert.apply(keySwap);
    }
}
