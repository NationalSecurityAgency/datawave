package datawave.query.iterator.builder;

import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.logic.IndexIterator;
import datawave.query.iterator.logic.IndexIteratorBridge;
import datawave.query.jexl.functions.FieldIndexAggregator;
import datawave.query.predicate.TimeFilter;
import datawave.query.util.TypeMetadata;

/**
 * A convenience class that aggregates a field, value, source iterator, normalizer mappings, index only fields, data type filter and key transformer when
 * traversing a subtree in a query. This allows arbitrary ordering of the arguments.
 *
 */
public class IndexIteratorBuilder extends AbstractIteratorBuilder {

    protected SortedKeyValueIterator<Key,Value> source;
    protected TypeMetadata typeMetadata;
    protected Predicate<Key> datatypeFilter = Predicates.alwaysTrue();
    protected TimeFilter timeFilter = TimeFilter.alwaysTrue();
    protected FieldIndexAggregator keyTform;
    protected Set<String> fieldsToAggregate;

    public void setSource(final SortedKeyValueIterator<Key,Value> source) {
        this.source = source;
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

    public Predicate<Key> getDatatypeFilter() {
        return datatypeFilter;
    }

    public void setDatatypeFilter(Predicate<Key> datatypeFilter) {
        this.datatypeFilter = datatypeFilter;
    }

    public TimeFilter getTimeFilter() {
        return timeFilter;
    }

    public void setTimeFilter(TimeFilter timeFilter) {
        this.timeFilter = timeFilter;
    }

    public FieldIndexAggregator getKeyTransform() {
        return keyTform;
    }

    public void setKeyTransform(FieldIndexAggregator keyTform) {
        this.keyTform = keyTform;
    }

    public IndexIterator newIndexIterator(Text field, Text value, SortedKeyValueIterator<Key,Value> source, TimeFilter timeFilter, TypeMetadata typeMetadata,
                    boolean buildDocument, Predicate<Key> datatypeFilter, FieldIndexAggregator aggregator) {
        return IndexIterator.builder(field, value, source).withTimeFilter(timeFilter).withTypeMetadata(typeMetadata).shouldBuildDocument(buildDocument)
                        .withDatatypeFilter(datatypeFilter).withAggregation(aggregator).build();
    }

    @SuppressWarnings("unchecked")
    public NestedIterator<Key> build() {
        if (notNull(field, value, source, datatypeFilter, keyTform, timeFilter, getField(), getNode())) {

            boolean canBuildDocument = this.fieldsToAggregate == null ? false : this.fieldsToAggregate.contains(field);
            if (forceDocumentBuild) {
                canBuildDocument = true;
            }
            IndexIteratorBridge itr = new IndexIteratorBridge(newIndexIterator(new Text(field), new Text(value), source, this.timeFilter, this.typeMetadata,
                            canBuildDocument, this.datatypeFilter, this.keyTform), getNode(), getField());
            field = null;
            value = null;
            source = null;
            timeFilter = null;
            datatypeFilter = null;
            keyTform = null;
            node = null;
            return itr;
        } else {
            StringBuilder msg = new StringBuilder(256);
            msg.append("Cannot build iterator-- a field was null!\n");
            if (field == null) {
                msg.append("\tField was null!\n");
            }
            if (value == null) {
                msg.append("\tValue was null!\n");
            }
            if (source == null) {
                msg.append("\tSource was null!\n");
            }
            if (getNode() == null) {
                msg.append("\tNode was null!\n");
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
