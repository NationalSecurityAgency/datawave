package datawave.query.iterator.profile;

import java.io.IOException;
import java.util.Collection;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.log4j.Logger;

public class SourceTrackingIterator extends WrappingIterator {
    protected QuerySpan querySpan;
    private Logger log = Logger.getLogger(QuerySpan.class);

    public SourceTrackingIterator(QuerySpan span, SortedKeyValueIterator<Key,Value> kv) {
        setSource(kv);
        querySpan = span;
    }

    @Override
    public void next() throws IOException {
        querySpan.next();
        super.next();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        querySpan.seek();
        super.seek(range, columnFamilies, inclusive);
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        // deep copy the source
        return new SourceTrackingIterator(querySpan.createSource(), getSource().deepCopy(env));
    }

    public QuerySpan getQuerySpan() {
        return querySpan;
    }
}
