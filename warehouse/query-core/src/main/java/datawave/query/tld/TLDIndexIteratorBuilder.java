package datawave.query.tld;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import com.google.common.base.Predicate;

import datawave.query.iterator.builder.IndexIteratorBuilder;
import datawave.query.iterator.logic.IndexIterator;
import datawave.query.jexl.functions.FieldIndexAggregator;
import datawave.query.predicate.TimeFilter;
import datawave.query.util.TypeMetadata;

public class TLDIndexIteratorBuilder extends IndexIteratorBuilder {

    @Override
    public IndexIterator newIndexIterator(Text field, Text value, SortedKeyValueIterator<Key,Value> source, TimeFilter timeFilter, TypeMetadata typeMetadata,
                    boolean buildDocument, Predicate<Key> datatypeFilter, FieldIndexAggregator aggregator) {
        //  @formatter:off
        return TLDIndexIterator.builder(field, value, source)
                .withTimeFilter(timeFilter)
                .withTypeMetadata(typeMetadata)
                .shouldBuildDocument(buildDocument)
                .withDatatypeFilter(datatypeFilter)
                .withAggregation(aggregator)
                .build();
        //  @formatter:on
    }
}
