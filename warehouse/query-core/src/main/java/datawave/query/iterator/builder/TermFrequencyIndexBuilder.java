package datawave.query.iterator.builder;

import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl2.parser.JexlNode;

import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.logic.IndexIteratorBridge;
import datawave.query.iterator.logic.TermFrequencyIndexIterator;
import datawave.query.jexl.functions.TermFrequencyAggregator;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.predicate.TimeFilter;
import datawave.query.util.TypeMetadata;

/**
 * A convenience class that aggregates a field, value, source iterator, normalizer mappings, index only fields, data type filter and key transformer when
 * traversing a subtree in a query. This allows arbitrary ordering of the arguments.
 *
 */
public class TermFrequencyIndexBuilder implements IteratorBuilder {
    protected String field;
    protected Range range;
    protected SortedKeyValueIterator<Key,Value> source;
    protected TypeMetadata typeMetadata;
    protected Set<String> compositeFields;
    protected TimeFilter timeFilter = TimeFilter.alwaysTrue();
    protected Set<String> fieldsToAggregate;
    protected EventDataQueryFilter attrFilter;
    protected TermFrequencyAggregator termFrequencyAggregator;
    protected IteratorEnvironment iteratorEnvironment;
    protected JexlNode node;

    public void setNode(JexlNode node) {
        this.node = node;
    }

    public JexlNode getNode() {
        return node;
    }

    public void setSource(final SortedKeyValueIterator<Key,Value> source) {
        this.source = source;
    }

    public String getField() {
        return this.field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public void setRange(Range range) {
        this.range = range;
    }

    public TypeMetadata getTypeMetadata() {
        return typeMetadata;
    }

    public void setTypeMetadata(TypeMetadata typeMetadata) {
        this.typeMetadata = typeMetadata;
    }

    public Set<String> getFieldsToAggregate() {
        return fieldsToAggregate;
    }

    public void setFieldsToAggregate(Set<String> fields) {
        fieldsToAggregate = fields;
    }

    public Set<String> getCompositeFields() {
        return compositeFields;
    }

    public void setCompositeFields(Set<String> compositeFields) {
        this.compositeFields = compositeFields;
    }

    public TimeFilter getTimeFilter() {
        return timeFilter;
    }

    public void setTimeFilter(TimeFilter timeFilter) {
        this.timeFilter = timeFilter;
    }

    public EventDataQueryFilter getAttrFilter() {
        return attrFilter;
    }

    public void setAttrFilter(EventDataQueryFilter attrFilter) {
        this.attrFilter = attrFilter;
    }

    public void setTermFrequencyAggregator(TermFrequencyAggregator termFrequencyAggregator) {
        this.termFrequencyAggregator = termFrequencyAggregator;
    }

    public void setEnv(IteratorEnvironment env) {
        this.iteratorEnvironment = env;
    }

    @SuppressWarnings("unchecked")
    public NestedIterator<Key> build() {
        if (notNull(field, range, source, timeFilter)) {
            IndexIteratorBridge itr = new IndexIteratorBridge(
                            new TermFrequencyIndexIterator(range, source, this.timeFilter, this.typeMetadata,
                                            this.fieldsToAggregate == null ? false : this.fieldsToAggregate.contains(field), termFrequencyAggregator),
                            getNode(), getField());
            field = null;
            range = null;
            source = null;
            timeFilter = null;
            node = null;
            return itr;
        } else {
            StringBuilder msg = new StringBuilder(256);
            msg.append("Cannot build iterator-- a field was null!\n");
            if (field == null) {
                msg.append("\tField was null!\n");
            }
            if (range == null) {
                msg.append("\tValue was null!\n");
            }
            if (source == null) {
                msg.append("\tSource was null!\n");
            }
            msg.setLength(msg.length() - 1);
            throw new IllegalStateException(msg.toString());
        }
    }

    public static boolean notNull(Object... os) {
        for (Object o : os) {
            if (o == null) {
                return false;
            }
        }
        return true;
    }
}
