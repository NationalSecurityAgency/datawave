package datawave.query.iterator.facets;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import datawave.query.attributes.Document;
import datawave.query.tables.facets.FacetTableFunction;
import datawave.query.util.SortedKeyValueIteratorToIterator;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

public class FacetedTableIterator extends DynamicFacetIterator {

    private static final Logger log = Logger.getLogger(FacetedTableIterator.class);

    protected SortedKeyValueIterator<Key,Value> pivotSource;

    protected SortedKeyValueIterator<Key,Value> facetSource;

    protected FacetTableFunction function;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        merge = true;

        super.init(source, options, env);
        // we're doing this so that
        pivotSource = source;

        this.myEnvironment = env;

        facetSource = pivotSource.deepCopy(env);

        function = new FacetTableFunction();

    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Iterator<Entry<Key,Document>> getDocumentIterator(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {

        facetSource.seek(range, columnFamilies, inclusive);

        return Iterators.transform(new SortedKeyValueIteratorToIterator(facetSource), function);

    }
}
