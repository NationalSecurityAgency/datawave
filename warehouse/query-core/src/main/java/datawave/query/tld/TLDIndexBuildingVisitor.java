package datawave.query.tld;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import datawave.data.type.NoOpType;
import datawave.query.Constants;
import datawave.query.iterator.builder.AbstractIteratorBuilder;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlASTHelper.IdentifierOpLiteral;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.functions.EventFieldAggregator;
import datawave.query.jexl.functions.TLDEventFieldAggregator;
import datawave.query.jexl.functions.TermFrequencyAggregator;
import datawave.query.jexl.visitors.IteratorBuildingVisitor;
import datawave.query.predicate.ChainableEventDataQueryFilter;
import datawave.query.predicate.TLDTermFrequencyEventDataQueryFilter;
import datawave.query.util.IteratorToSortedKeyValueIterator;

public class TLDIndexBuildingVisitor extends IteratorBuildingVisitor {
    private static final Logger log = Logger.getLogger(TLDIndexBuildingVisitor.class);

    @Override
    public Object visit(ASTNENode node, Object data) {
        // We have no parent already defined
        if (data == null) {
            // We don't support querying only on a negation
            throw new IllegalStateException("Root node cannot be a negation");
        }
        TLDIndexIteratorBuilder builder = new TLDIndexIteratorBuilder();
        builder.setSource(deepCopySource());
        builder.setTypeMetadata(typeMetadata);
        builder.setDatatypeFilter(getDatatypeFilter());
        builder.setKeyTransform(getFiAggregator());
        builder.setTimeFilter(timeFilter);
        builder.setNode(node);
        node.childrenAccept(this, builder);

        builder.buildDocument(shouldBuildDocument(builder.getField()));

        // A EQNode may be of the form FIELD == null. The evaluation can
        // handle this, so we should just not build an IndexIterator for it.
        if (null == builder.getValue()) {
            if (isQueryFullySatisfied == true) {
                throw new RuntimeException("Determined that isQueryFullySatisfied should be false, but it was not preset to false by the SatisfactionVisitor");
            }
            return null;
        }

        AbstractIteratorBuilder iterators = (AbstractIteratorBuilder) data;
        // Add the negated IndexIteratorBuilder to the parent as an *exclude*
        if (!iterators.hasSeen(builder.getField(), builder.getValue()) && includeReferences.contains(builder.getField())
                        && !excludeReferences.contains(builder.getField())) {
            iterators.addExclude(builder.build());
        } else {
            if (isQueryFullySatisfied == true) {
                throw new RuntimeException("Determined that isQueryFullySatisfied should be false, but it was not preset to false by the SatisfactionVisitor");
            }
        }

        return null;
    }

    @Override
    protected void seekIndexOnlyDocument(SortedKeyValueIterator<Key,Value> kvIter, ASTEQNode node) throws IOException {
        if (null != rangeLimiter && limitLookup) {

            Key newStartKey = getKey(node);

            kvIter.seek(new Range(newStartKey, true, newStartKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL), false), Collections.emptyList(), false);

        }
    }

    @Override
    protected SortedKeyValueIterator<Key,Value> createIndexOnlyKey(ASTEQNode node) throws IOException {
        Key newStartKey = getKey(node);

        IdentifierOpLiteral op = JexlASTHelper.getIdentifierOpLiteral(node);
        if (null == op || null == op.getLiteralValue()) {
            // deep copy since this is likely a null literal
            return deepCopySource();
        }

        String fn = op.deconstructIdentifier();
        String literal = String.valueOf(op.getLiteralValue());

        if (log.isTraceEnabled()) {
            log.trace("createIndexOnlyKey for " + fn + " " + literal + " " + newStartKey);
        }
        List<Entry<Key,Value>> kv = Lists.newArrayList();
        if (null != limitedMap.get(Maps.immutableEntry(fn, literal))) {
            kv.add(limitedMap.get(Maps.immutableEntry(fn, literal)));
        } else {

            SortedKeyValueIterator<Key,Value> mySource = limitedSource;
            // if source size > 0, we are free to use up to that number for this query
            if (source.getSourceSize() > 0)
                mySource = deepCopySource();

            mySource.seek(new Range(newStartKey, true, newStartKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL), false), Collections.emptyList(), false);

            if (mySource.hasTop()) {
                kv.add(Maps.immutableEntry(mySource.getTopKey(), Constants.NULL_VALUE));

            }
        }

        return new IteratorToSortedKeyValueIterator(kv.iterator());
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {

        TLDIndexIteratorBuilder builder = new TLDIndexIteratorBuilder();
        node.childrenAccept(this, builder);

        // verify that the field exists and is indexed
        if (builder.getField() == null || isUnindexed(builder.getField())) {
            if (isQueryFullySatisfied) {
                log.warn("Determined that isQueryFullySatisfied should be false, but it was not preset to false in the SatisfactionVisitor");
            }
            return null;
        }

        // check for the case 'FIELD == null'
        if (builder.getValue() == null) {
            if (isQueryFullySatisfied) {
                throw new RuntimeException("Determined that isQueryFullySatisfied should be false, but it was not preset to false by the SatisfactionVisitor");
            }
            return null;
        }

        // check to see if there is a mismatch between included and exclude references.
        // note: this is a lift and shift of old code and probably doesn't work as intended..
        if (!includeReferences.contains(builder.getField()) && excludeReferences.contains(builder.getField())) {
            throw new IllegalStateException(builder.getField() + " is a disallowlisted reference.");
        }

        // We have no parent already defined
        if (data == null) {
            // Make this EQNode the root
            loadBuilder(builder, data, node);
            root = builder.build();
            if (log.isTraceEnabled()) {
                log.trace("Build IndexIterator: " + root);
            }
        } else {
            AbstractIteratorBuilder iterators = (AbstractIteratorBuilder) data;
            // Add this IndexIterator to the parent
            if (!iterators.hasSeen(builder.getField(), builder.getValue()) && includeReferences.contains(builder.getField())
                            && !excludeReferences.contains(builder.getField())) {
                loadBuilder(builder, data, node);
                iterators.addInclude(builder.build());
            } else {
                if (isQueryFullySatisfied) {
                    log.warn("Determined that isQueryFullySatisfied should be false, but it was not preset to false by the SatisfactionVisitor");
                }
            }
        }

        return null;
    }

    @Override
    protected EventFieldAggregator getEventFieldAggregator(String field, ChainableEventDataQueryFilter filter) {
        return new TLDEventFieldAggregator(field, filter, eventNextSeek, typeMetadata, NoOpType.class.getName());
    }

    /**
     * Use fieldsToAggregate instead of indexOnlyFields because this enables TLDs to return non-event tokens as part of the user document
     *
     * @param identifier
     *            the field to be aggregated
     * @param filterChain
     *            a {@link ChainableEventDataQueryFilter}
     * @param maxNextCount
     *            the maximum number of next calls before a seek is issued
     * @return a {@link TermFrequencyAggregator} loaded with the provided filter
     */
    @Override
    protected TermFrequencyAggregator buildTermFrequencyAggregator(String identifier, ChainableEventDataQueryFilter filterChain, int maxNextCount) {

        Set<String> toAggregate = fieldsToAggregate.contains(identifier) ? Collections.singleton(identifier) : Collections.emptySet();
        filterChain.addFilter(new TLDTermFrequencyEventDataQueryFilter(indexOnlyFields, toAggregate));

        return new TLDTermFrequencyAggregator(toAggregate, filterChain, tfNextSeek);
    }

    /**
     * Range should be build to encompass the entire TLD
     *
     * @param range
     *            non-null literal range to generate an FI range from
     * @return a range
     */
    @Override
    protected Range getFiRangeForTF(LiteralRange<?> range) {
        Key startKey = rangeLimiter.getStartKey();

        StringBuilder strBuilder = new StringBuilder("fi");
        strBuilder.append(NULL_DELIMETER).append(range.getFieldName());
        Text cf = new Text(strBuilder.toString());

        strBuilder = new StringBuilder(range.getLower().toString());

        strBuilder.append(NULL_DELIMETER).append(startKey.getColumnFamily());
        Text cq = new Text(strBuilder.toString());

        Key seekBeginKey = new Key(startKey.getRow(), cf, cq, startKey.getTimestamp());

        strBuilder = new StringBuilder(range.getUpper().toString());

        strBuilder.append(NULL_DELIMETER).append(startKey.getColumnFamily() + Constants.MAX_UNICODE_STRING);
        cq = new Text(strBuilder.toString());

        Key seekEndKey = new Key(startKey.getRow(), cf, cq, startKey.getTimestamp());

        return new Range(seekBeginKey, true, seekEndKey, true);
    }
}
