package datawave.query.iterator.builder;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.filter.field.index.FieldIndexFilter;
import datawave.query.iterator.logic.IndexIterator;
import datawave.query.iterator.logic.IndexIteratorBridge;
import datawave.query.jexl.functions.FieldIndexAggregator;
import datawave.query.predicate.TimeFilter;
import datawave.query.util.TypeMetadata;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import java.util.Set;

/**
 * A convenience class that aggregates a field, value, source iterator, normalizer mappings, index only fields, data type filter and key transformer when
 * traversing a subtree in a query. This allows arbitrary ordering of the arguments.
 * 
 */
public class IndexIteratorBuilder extends AbstractIteratorBuilder {
    
    protected SortedKeyValueIterator<Key,Value> source;
    protected TypeMetadata typeMetadata;
    protected TypeMetadata typeMetadataWithNonIndexed;
    protected Predicate<Key> datatypeFilter = Predicates.alwaysTrue();
    protected TimeFilter timeFilter = TimeFilter.alwaysTrue();
    protected FieldIndexAggregator keyTform;
    protected Set<String> fieldsToAggregate;
    protected FieldIndexFilter fieldIndexFilter;
    
    public void setSource(final SortedKeyValueIterator<Key,Value> source) {
        this.source = source;
    }
    
    public TypeMetadata getTypeMetadata() {
        return typeMetadata;
    }
    
    public void setTypeMetadata(TypeMetadata typeMetadata) {
        this.typeMetadata = typeMetadata;
    }
    
    public TypeMetadata getTypeMetadataWithNonIndexed() {
        return typeMetadataWithNonIndexed;
    }
    
    public void setTypeMetadataWithNonIndexed(TypeMetadata typeMetadataWithNonIndexed) {
        this.typeMetadataWithNonIndexed = typeMetadataWithNonIndexed;
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
    
    public FieldIndexFilter getFieldIndexFilter() {
        return fieldIndexFilter;
    }
    
    public void setFieldIndexFilter(FieldIndexFilter fieldIndexFilter) {
        this.fieldIndexFilter = fieldIndexFilter;
    }
    
    public IndexIterator newIndexIterator(Text field, Text value, SortedKeyValueIterator<Key,Value> source, TimeFilter timeFilter, TypeMetadata typeMetadata,
                    boolean buildDocument, Predicate<Key> datatypeFilter, FieldIndexAggregator aggregator, FieldIndexFilter fieldIndexFilter) {
        return new IndexIterator(field, value, source, timeFilter, typeMetadata, buildDocument, datatypeFilter, aggregator, fieldIndexFilter);
    }
    
    @SuppressWarnings("unchecked")
    public NestedIterator<Key> build() {
        if (notNull(field, value, source, datatypeFilter, keyTform, timeFilter)) {
            
            boolean canBuildDocument = this.fieldsToAggregate == null ? false : this.fieldsToAggregate.contains(field);
            if (forceDocumentBuild) {
                canBuildDocument = true;
            }
            IndexIteratorBridge itr = new IndexIteratorBridge(newIndexIterator(new Text(field), new Text(value), source, this.timeFilter, this.typeMetadata,
                            canBuildDocument, this.datatypeFilter, this.keyTform, this.fieldIndexFilter));
            field = null;
            value = null;
            source = null;
            timeFilter = null;
            datatypeFilter = null;
            keyTform = null;
            fieldIndexFilter = null;
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
