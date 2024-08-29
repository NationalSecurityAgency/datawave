package datawave.core.iterators;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Sets;

import datawave.marking.MarkingFunctions;

/**
 * <p>
 * A simple iterator that will count the number of k/v pairs returned by the iterator beneath it on the stack.
 * </p>
 *
 * <p>
 * This iterator will only ever return one key/value pair.
 * <ul>
 * <li>The key is start_key of the range passed to seek() which ensures that the key is not automatically filtered out due to falling outside the range.</li>
 * <li>The value is a {@link VLongWritable} which is the number of k/v pairs obtained below this iterator.</li>
 * </ul>
 *
 * <p>
 * When using a {@link BatchScanner}, be aware that this will return <code>n</code> counts, where <code>n</code> is the number of ranges set. It is up to the
 * client to sum the counts for each range together.
 */
public class ResultCountingIterator extends WrappingIterator {
    private static final Logger log = Logger.getLogger(ResultCountingIterator.class);
    private final Cache<Text,ColumnVisibility> CV_CACHE = CacheBuilder.newBuilder().concurrencyLevel(1).maximumSize(100).build();

    private static final Ticker zeroTicker = new Ticker() {
        @Override
        public long read() {
            return 0;
        }
    };
    private int count;

    private Key currentTopKey = null;
    private Kryo kryo = new Kryo();

    private String threadName = null;
    protected Set<ColumnVisibility> columnVisibilities = Sets.newHashSet();

    public ResultCountingIterator() {
        threadName = Thread.currentThread().getName();
    }

    public ResultCountingIterator(ResultCountingIterator other, IteratorEnvironment env) {
        this();
        setSource(other.getSource().deepCopy(env));
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace(threadName + ": init");
        }
        setSource(source.deepCopy(env));
        this.count = 0;
    }

    @Override
    public boolean hasTop() {
        return 0 < count;
    }

    @Override
    public void next() throws IOException {
        if (getSource().hasTop()) {
            getSource().next();
            consume();
        } else {
            // We're done. No more counts
            this.count = 0;
        }
    }

    /**
     * A <code>seek</code> will reset the count made by this iterator.
     */
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace(threadName + ": seeking to : " + range);
        }
        getSource().seek(range, columnFamilies, inclusive);
        consume();
    }

    public void consume() throws IOException {
        Stopwatch consumeSW = null, processResultSW = null, ioWaitSW = null;

        if (log.isTraceEnabled()) {
            consumeSW = Stopwatch.createUnstarted();
            processResultSW = Stopwatch.createUnstarted();
            ioWaitSW = Stopwatch.createUnstarted();
        } else {
            consumeSW = Stopwatch.createUnstarted(zeroTicker);
            processResultSW = Stopwatch.createUnstarted(zeroTicker);
            ioWaitSW = Stopwatch.createUnstarted(zeroTicker);
        }

        consumeSW.start();
        ioWaitSW.start();

        this.count = 0;
        while (getSource().hasTop()) {
            ioWaitSW.stop();
            processResultSW.start();

            if (getSource().getTopKey() != null) {
                final Text cvholder = new Text();

                this.currentTopKey = getSource().getTopKey();
                this.currentTopKey.getColumnVisibility(cvholder);

                // Merge the ColumnVisibilities
                // Do not count the record if we can't parse its ColumnVisibility
                try {
                    ColumnVisibility cv = CV_CACHE.get(cvholder, () -> new ColumnVisibility(cvholder));

                    columnVisibilities.add(cv);
                } catch (ExecutionException e) {
                    log.error("Error parsing ColumnVisibility of key", e);
                    continue;
                }

                this.count++;
            }

            processResultSW.stop();
            ioWaitSW.start();

            try {
                getSource().next();
            } catch (IOException e) {
                log.error("IOException in calling next on the source", e);
                break;
            }
        }

        ioWaitSW.stop();
        consumeSW.stop();

        if (log.isDebugEnabled()) {
            log.debug(threadName + ": Returning a count of " + this.count);
        }

        if (log.isTraceEnabled()) {
            log.trace(threadName + ": Total consume() time: " + consumeSW.elapsed(TimeUnit.MILLISECONDS));
            log.trace(threadName + ": Total next()/hasNext() time: " + ioWaitSW.elapsed(TimeUnit.MILLISECONDS));
            log.trace(threadName + ": Total internal time: " + processResultSW.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    @Override
    public Key getTopKey() {
        return currentTopKey; // This is the unchanged key that our source iterator returned.
    }

    /**
     * Kryo serialized object ResultCountTuple which contains a {@code long count, int visibilityLength, byte[] visibility}
     *
     * @return serialized form of the count and rolled up visibility
     */
    @Override
    public Value getTopValue() {
        if (null == currentTopKey) {
            return null;
        }

        Stopwatch sw = null;

        if (log.isTraceEnabled()) {
            log.trace(threadName + ": getTopValue()");
            sw = Stopwatch.createUnstarted();
        } else {
            sw = Stopwatch.createUnstarted(zeroTicker);
        }

        sw.start();

        ColumnVisibility cv = null;

        try {
            cv = MarkingFunctions.Factory.createMarkingFunctions().combine(columnVisibilities);
        } catch (MarkingFunctions.Exception e) {
            log.error("Could not create combined columnVisibility for the count", e);
            return null;
        }

        ResultCountTuple result = new ResultCountTuple(this.count, cv);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output kryoOutput = new Output(baos);
        kryo.writeObject(kryoOutput, result);
        kryoOutput.close();

        sw.stop();
        if (log.isTraceEnabled()) {
            log.trace(threadName + ": Elapsed getTopValue(): " + sw.elapsed(TimeUnit.MILLISECONDS));
        }

        return new Value(baos.toByteArray());
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new ResultCountingIterator(this, env);
    }

    /**
     * Simple tuple class implementing Kryo Serialization. Will hold the count and rolled up column visibility.
     *
     * The serialized object will be of the form: long - count int - length of visibility expression byte array byte[] - rolled up ColumnVisibility
     */
    public static class ResultCountTuple implements KryoSerializable {
        long count;
        ColumnVisibility visibility;

        public ResultCountTuple() {
            // need default constructor for kryo
        }

        public ResultCountTuple(long count, ColumnVisibility visibility) {
            this.count = count;
            this.visibility = visibility;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        public ColumnVisibility getVisibility() {
            return visibility;
        }

        public void setVisibility(ColumnVisibility visibility) {
            this.visibility = visibility;
        }

        @Override
        public void write(Kryo kryo, Output output) {
            output.writeLong(count);
            byte[] expression = visibility.getExpression();
            output.writeInt(expression.length);
            output.writeBytes(expression);
        }

        @Override
        public void read(Kryo kryo, Input input) {
            this.count = input.readLong();
            int expressionLength = input.readInt();
            this.visibility = new ColumnVisibility(input.readBytes(expressionLength));
        }

        @Override
        public String toString() {
            return "ResultCountTuple{" + "count=" + count + ", visibility=" + visibility + '}';
        }
    }
}
