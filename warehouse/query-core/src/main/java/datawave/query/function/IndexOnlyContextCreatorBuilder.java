package datawave.query.function;

import com.google.common.collect.Multimap;
import datawave.query.composite.CompositeMetadata;
import datawave.query.iterator.QueryOptions;
import datawave.query.jexl.visitors.IteratorBuildingVisitor;
import datawave.query.util.TypeMetadata;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.JexlNode;

import java.util.Collection;

/**
 * Builder for IndexOnlyContextCreator
 */
public class IndexOnlyContextCreatorBuilder {
    private Collection<String> variables;
    private TypeMetadata typeMetadata;
    private CompositeMetadata compositeMetadata;
    private Range range;
    private IteratorBuildingVisitor iteratorBuildingVisitor;
    private Multimap<String,JexlNode> delayedNonEventFieldMap;
    private Equality equality;
    private Collection<ByteSequence> columnFamilies;
    private boolean inclusive;
    private SortedKeyValueIterator<Key,Value> source;
    private JexlContextCreator.JexlContextValueComparator comparatorFactory;
    private QueryOptions options;

    public IndexOnlyContextCreator build() {
        return new IndexOnlyContextCreator(source, range, typeMetadata, compositeMetadata, options, variables, iteratorBuildingVisitor, delayedNonEventFieldMap,
                        equality, columnFamilies, inclusive, comparatorFactory);
    }

    public IndexOnlyContextCreatorBuilder setVariables(Collection<String> variables) {
        this.variables = variables;
        return this;
    }

    public IndexOnlyContextCreatorBuilder setComparatorFactory(JexlContextCreator.JexlContextValueComparator comparatorFactory) {
        this.comparatorFactory = comparatorFactory;
        return this;
    }

    public IndexOnlyContextCreatorBuilder setOptions(QueryOptions options) {
        this.options = options;
        return this;
    }

    public IndexOnlyContextCreatorBuilder setSource(SortedKeyValueIterator<Key,Value> source) {
        this.source = source;
        return this;
    }

    public IndexOnlyContextCreatorBuilder setTypeMetadata(TypeMetadata typeMetadata) {
        this.typeMetadata = typeMetadata;
        return this;
    }

    public IndexOnlyContextCreatorBuilder setCompositeMetadata(CompositeMetadata compositeMetadata) {
        this.compositeMetadata = compositeMetadata;
        return this;
    }

    public IndexOnlyContextCreatorBuilder setRange(Range range) {
        this.range = range;
        return this;
    }

    public IndexOnlyContextCreatorBuilder setIteratorBuildingVisitor(IteratorBuildingVisitor iteratorBuildingVisitor) {
        this.iteratorBuildingVisitor = iteratorBuildingVisitor;
        return this;
    }

    public IndexOnlyContextCreatorBuilder setDelayedNonEventFieldMap(Multimap<String,JexlNode> delayedNonEventFieldMap) {
        this.delayedNonEventFieldMap = delayedNonEventFieldMap;
        return this;
    }

    public IndexOnlyContextCreatorBuilder setEquality(Equality equality) {
        this.equality = equality;
        return this;
    }

    public IndexOnlyContextCreatorBuilder setColumnFamilies(Collection<ByteSequence> columnFamilies) {
        this.columnFamilies = columnFamilies;
        return this;
    }

    public IndexOnlyContextCreatorBuilder setInclusive(boolean inclusive) {
        this.inclusive = inclusive;
        return this;
    }
}
