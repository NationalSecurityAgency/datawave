package datawave.query.iterator.errors;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

import datawave.query.iterator.QueryInformationIterator;

/**
 * An identity iterator which does nothing but defer to its source.
 *
 */
public class ErrorReportingIterator extends QueryInformationIterator {

    private static final Logger log = Logger.getLogger(ErrorReportingIterator.class);

    public ErrorReportingIterator() {
        super();
    }

    public ErrorReportingIterator(ErrorReportingIterator other, IteratorEnvironment env) {
        super(other, env);
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
    }

    @Override
    public void next() throws IOException {
        Exception exc = null;
        try {
            super.next();

        } catch (UnindexedException e) {
            if (reportErrors)
                errorKey = new ErrorKey(ErrorType.UNINDEXED_FIELD);
            else {
                exc = e;
            }
        } catch (RuntimeException | IOException e) {
            exc = e;
        }
        if (null != exc) {
            log.error("Caught exception on next: " + info, exc);
            throw new IOException(exc);
        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        Exception exc = null;
        try {
            super.seek(range, columnFamilies, inclusive);
        } catch (UnindexedException e) {
            if (reportErrors)
                errorKey = new ErrorKey(ErrorType.UNINDEXED_FIELD);
            else {
                exc = e;
            }
        } catch (RuntimeException | IOException e) {
            exc = e;
        }

        if (null != exc) {
            log.error("Caught exception on seek: " + info, exc);
            throw new IOException(exc);
        }
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new ErrorReportingIterator(this, env);
    }

}
