package nsa.datawave.query.rewrite.tld;

import com.google.common.base.Predicate;
import nsa.datawave.query.rewrite.iterator.builder.IndexIteratorBuilder;
import nsa.datawave.query.rewrite.iterator.logic.IndexIterator;
import nsa.datawave.query.rewrite.iterator.logic.IndexIteratorBridge;

import nsa.datawave.query.rewrite.jexl.functions.FieldIndexAggregator;
import nsa.datawave.query.rewrite.predicate.TimeFilter;
import nsa.datawave.query.util.TypeMetadata;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

public class TLDIndexIteratorBuilder extends IndexIteratorBuilder {
    @Override
    public IndexIterator newIndexIterator(Text field, Text value, SortedKeyValueIterator<Key,Value> source, TimeFilter timeFilter, TypeMetadata typeMetadata,
                    boolean buildDocument, Predicate<Key> datatypeFilter, FieldIndexAggregator aggregator) {
        return new TLDIndexIterator(field, value, source, timeFilter, typeMetadata, buildDocument, datatypeFilter, aggregator);
    }
    
}
