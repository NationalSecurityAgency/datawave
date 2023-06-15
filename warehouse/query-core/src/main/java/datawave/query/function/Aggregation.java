package datawave.query.function;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import datawave.query.attributes.Document;
import datawave.query.composite.CompositeMetadata;
import datawave.query.iterator.aggregation.DocumentData;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.predicate.TimeFilter;
import datawave.query.util.TypeMetadata;
import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

import java.util.Map.Entry;

public class Aggregation implements Function<Entry<DocumentData,Document>,Entry<Key,Document>> {
    private static final Logger log = Logger.getLogger(Aggregation.class);

    private TypeMetadata typeMetadata;
    private CompositeMetadata compositeMetadata;
    private TimeFilter timeFilter;
    private EventDataQueryFilter attrFilter;

    private boolean includeGroupingContext = false;
    private boolean includeRecordId = false;

    protected boolean disableIndexOnlyDocuments = false;

    /**
     * should documents track sizes
     */
    private boolean trackSizes = true;

    // Need to provide the mapping
    @SuppressWarnings("unused")
    private Aggregation() {}

    public Aggregation(TimeFilter timeFilter, TypeMetadata typeMetadata, CompositeMetadata compositeMetadata, boolean includeGroupingContext,
                    boolean includeRecordId, boolean disableIndexOnlyDocuments, EventDataQueryFilter attrFilter) {
        this(timeFilter, typeMetadata, compositeMetadata, includeGroupingContext, includeRecordId, disableIndexOnlyDocuments, attrFilter, true);
    }

    public Aggregation(TimeFilter timeFilter, TypeMetadata typeMetadata, CompositeMetadata compositeMetadata, boolean includeGroupingContext,
                    boolean includeRecordId, boolean disableIndexOnlyDocuments, EventDataQueryFilter attrFilter, boolean trackSizes) {
        Preconditions.checkNotNull(timeFilter);

        this.timeFilter = timeFilter;
        this.typeMetadata = typeMetadata;
        this.compositeMetadata = compositeMetadata;
        this.includeGroupingContext = includeGroupingContext;
        this.includeRecordId = includeRecordId;
        this.attrFilter = attrFilter;
        this.disableIndexOnlyDocuments = disableIndexOnlyDocuments;
        this.trackSizes = trackSizes;
    }

    @Override
    public Entry<Key,Document> apply(Entry<DocumentData,Document> from) {
        DocumentData docData = from.getKey();

        // set the document context on the attribute filter
        if (attrFilter != null) {
            attrFilter.startNewDocument(docData.getKey());
        }

        // Only load attributes for this document that fall within the expected date range
        Document d = new Document(docData.getKey(), docData.getDocKeys(), docData.isFromIndex(),
                        Iterators.filter(docData.getData().iterator(), timeFilter.getKeyValueTimeFilter()), this.typeMetadata, this.compositeMetadata,
                        this.includeGroupingContext, this.includeRecordId, this.attrFilter, true, trackSizes);

        if (log.isTraceEnabled()) {
            log.trace("disable index only docs? " + disableIndexOnlyDocuments + " , size is " + d.size());
        }

        if (null != from.getValue() && from.getValue().size() > 0 && (!disableIndexOnlyDocuments || d.size() > 0)) {
            d.putAll(from.getValue(), this.includeGroupingContext);
        }

        Key origKey = docData.getKey();

        if (log.isTraceEnabled()) {
            log.trace("Computed document for " + origKey + ": " + d);
        }

        return Maps.immutableEntry(origKey, d);
    }

}
