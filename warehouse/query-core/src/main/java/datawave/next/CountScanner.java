package datawave.next;

import java.io.ByteArrayOutputStream;
import java.util.Iterator;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

import datawave.core.iterators.ResultCountingIterator;
import datawave.core.query.configuration.QueryData;
import datawave.core.query.configuration.Result;
import datawave.next.scanner.DocumentScanner;
import datawave.next.scanner.DocumentScannerConfig;

/**
 * An extension of the {@link DocumentScanner}, it simply counts all the documents coming off the iterator.
 * <p>
 * There is room for an optimized version of this when the query is satisfiable from the field index where raw counts are returned from the
 * {@link DocIdQueryIterator}.
 */
public class CountScanner extends DocumentScanner {

    private static final Logger log = LoggerFactory.getLogger(CountScanner.class);

    private boolean completed = false;
    private Result sum;

    /**
     * Default constructor, will likely need to swap this out for a config object constructor
     *
     * @param config
     *            the {@link DocumentScannerConfig}
     * @param queryDataIterator
     *            the iterator of {@link QueryData}
     */
    public CountScanner(DocumentScannerConfig config, Iterator<QueryData> queryDataIterator) {
        super(config, queryDataIterator);
    }

    @Override
    public boolean hasNext() {
        while (super.hasNext()) {
            this.sum = super.next();
        }
        return !completed;
    }

    @Override
    public Result next() {
        if (!completed) {
            completed = true;
            Value value = serializeCount(config.getStats().getTotalResultsReturned(), sum.getKey().getColumnVisibilityParsed());
            return new Result<>(sum.getKey(), value);
        }

        return null;
    }

    private Value serializeCount(long count, ColumnVisibility cv) {
        log.info("count query found {} documents", count);

        Kryo kryo = new Kryo();
        ResultCountingIterator.ResultCountTuple result = new ResultCountingIterator.ResultCountTuple(count, cv);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output kryoOutput = new Output(baos);
        kryo.writeObject(kryoOutput, result);
        kryoOutput.close();

        return new Value(baos.toByteArray());
    }
}
