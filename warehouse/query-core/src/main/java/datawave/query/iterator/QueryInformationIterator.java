package datawave.query.iterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import datawave.query.iterator.errors.UnindexedException;
import datawave.query.Constants;
import datawave.query.exceptions.LoadAverageWatchException;
import datawave.query.iterator.errors.ErrorKey;
import datawave.query.iterator.errors.ErrorType;
import datawave.query.util.QueryInformation;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.log4j.Logger;

/**
 * An identity iterator which does nothing but defer to its source.
 *
 * The purpose of this iterator is to serve as a place-holder to store additional information regarding query metadata from the webservice (ID, name, logic,
 * etc).
 *
 */
public class QueryInformationIterator extends WrappingIterator {
    private static final Logger log = Logger.getLogger(QueryInformationIterator.class);

    protected static final String REPORT_ERRORS_OPT = "REPORT_ERRORS";

    protected QueryInformation info;

    protected ErrorKey errorKey = null;

    protected volatile boolean returned = false;

    protected boolean reportErrors = false;

    public QueryInformationIterator() {}

    public QueryInformationIterator(QueryInformationIterator other, IteratorEnvironment env) {
        this.setSource(other.getSource().deepCopy(env));
        this.info = other.info;
    }

    public static void setErrorReporting(IteratorSetting cfg) {
        cfg.addOption(REPORT_ERRORS_OPT, REPORT_ERRORS_OPT);
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        this.info = new QueryInformation(options);

        if (null != options.get(REPORT_ERRORS_OPT)) {
            reportErrors = true;
        }

        try {
            super.init(source, options, env);
        } catch (RuntimeException | IOException e) {
            log.error("Caught exception on init: " + info, e);
            throw e;
        }
    }

    public void setError(ErrorType type) {
        errorKey = new ErrorKey(type);

    }

    @Override
    public void next() throws IOException {
        try {
            if (null == errorKey)
                super.next();

        } catch (LoadAverageWatchException e) {
            if (reportErrors)
                errorKey = new ErrorKey(ErrorType.CONTINUE_WAIT);
            else {
                throw new IOException(e);
            }
        } catch (UnindexedException e) {
            if (reportErrors)
                errorKey = new ErrorKey(ErrorType.UNINDEXED_FIELD);
            else {
                throw new IOException(e);
            }
        } catch (RuntimeException | IOException e) {
            log.error("Caught exception on next: " + info, e);
            throw e;
        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        try {
            super.seek(range, columnFamilies, inclusive);
        } catch (LoadAverageWatchException e) {
            if (reportErrors)
                errorKey = new ErrorKey(ErrorType.CONTINUE_WAIT);
            else {
                throw new IOException(e);
            }
        } catch (UnindexedException e) {
            if (reportErrors)
                errorKey = new ErrorKey(ErrorType.UNINDEXED_FIELD);
            else {
                throw new IOException(e);
            }
        } catch (RuntimeException | IOException e) {
            log.error("Caught exception on seek: " + info, e);
            throw e;
        }

    }

    @Override
    public Key getTopKey() {
        if (null != errorKey) {
            returned = true;
            return errorKey;
        } else
            return super.getTopKey();
    }

    @Override
    public Value getTopValue() {
        if (null != errorKey)
            return Constants.EMPTY_VALUE;
        else
            return super.getTopValue();
    }

    @Override
    public boolean hasTop() {
        if (null != errorKey)
            return true;
        else {
            if (returned)
                return false;
            else
                return super.hasTop();
        }
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new QueryInformationIterator(this, env);
    }

}
