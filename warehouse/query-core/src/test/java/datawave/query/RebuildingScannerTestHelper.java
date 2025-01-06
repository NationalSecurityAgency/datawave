package datawave.query;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryBatchScanner;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.accumulo.inmemory.InMemoryScanner;
import datawave.accumulo.inmemory.InMemoryScannerBase;
import datawave.accumulo.inmemory.ScannerRebuilder;
import datawave.query.attributes.Document;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.iterator.profile.FinalDocumentTrackingIterator;

/**
 * This helper provides support for testing the teardown of iterators at randomly. Simply use the getConnector methods and all scanners created will contain an
 * iterator that will force the stack to be torn down.
 */
public class RebuildingScannerTestHelper {

    public enum TEARDOWN {
        NEVER(NeverTeardown.class),
        ALWAYS(AlwaysTeardown.class),
        RANDOM(RandomTeardown.class),
        EVERY_OTHER(EveryOtherTeardown.class),
        ALWAYS_SANS_CONSISTENCY(AlwaysTeardownWithoutConsistency.class),
        RANDOM_SANS_CONSISTENCY(RandomTeardownWithoutConsistency.class),
        EVERY_OTHER_SANS_CONSISTENCY(EveryOtherTeardownWithoutConsistency.class);

        private final Class<? extends TeardownListener> tclass;

        TEARDOWN(Class<? extends TeardownListener> tclass) {
            this.tclass = tclass;
        }

        public TeardownListener instance() {
            try {
                return tclass.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public enum INTERRUPT {
        NEVER(NeverInterrupt.class),
        RANDOM(RandomInterrupt.class),
        EVERY_OTHER(EveryOtherInterrupt.class),
        FI_EVERY_OTHER(FiEveryOtherInterrupt.class),
        RANDOM_HIGH(HighRandomInterrupt.class);

        private final Class<? extends InterruptListener> iclass;

        INTERRUPT(Class<? extends InterruptListener> iclass) {
            this.iclass = iclass;
        }

        public InterruptListener instance() {
            try {
                return iclass.newInstance();
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            } catch (InstantiationException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class InterruptIterator implements SortedKeyValueIterator<Key,Value> {
        private SortedKeyValueIterator<Key,Value> source;
        private InterruptListener interruptListener;
        private boolean initialized = false;

        public InterruptIterator() {
            // no-op
        }

        public InterruptIterator(InterruptIterator other, IteratorEnvironment env) {
            this.interruptListener = other.interruptListener;
            this.source = other.source.deepCopy(env);
        }

        @Override
        public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> map, IteratorEnvironment iteratorEnvironment) throws IOException {
            this.source = source;
        }

        @Override
        public boolean hasTop() {
            return source.hasTop();
        }

        @Override
        public void next() throws IOException {
            if (initialized && interruptListener != null && interruptListener.interrupt(source.getTopKey())) {
                throw new IterationInterruptedException("testing next interrupt");
            }

            source.next();
        }

        @Override
        public void seek(Range range, Collection<ByteSequence> collection, boolean inclusive) throws IOException {
            if (interruptListener != null && interruptListener.interrupt(null)) {
                throw new IterationInterruptedException("testing seek interrupt");
            }

            source.seek(range, collection, inclusive);

            initialized = true;
        }

        @Override
        public Key getTopKey() {
            return source.getTopKey();
        }

        @Override
        public Value getTopValue() {
            return source.getTopValue();
        }

        @Override
        public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
            return new InterruptIterator(this, env);
        }

        // must be set by the RebuildingIterator after each rebuild
        public void setInterruptListener(InterruptListener listener) {
            this.interruptListener = listener;
        }
    }

    public interface InterruptListener {
        boolean interrupt(Key key);

        void processedInterrupt(boolean interrupt);
    }

    /**
     * Never interrupts
     */
    public static class NeverInterrupt implements InterruptListener {
        @Override
        public boolean interrupt(Key key) {
            return false;
        }

        @Override
        public void processedInterrupt(boolean interrupt) {
            // no-op
        }
    }

    /**
     * Sets interrupting at 50% rate
     */
    public static class RandomInterrupt implements InterruptListener {
        protected final Random random = new Random();
        protected boolean interrupting = random.nextBoolean();

        @Override
        public boolean interrupt(Key key) {
            return interrupting;
        }

        @Override
        public void processedInterrupt(boolean interrupt) {
            interrupting = random.nextBoolean();
        }
    }

    /**
     * Sets interrupting at 95% rate
     */
    public static class HighRandomInterrupt extends RandomInterrupt {
        @Override
        public void processedInterrupt(boolean interrupt) {
            interrupting = random.nextInt(100) < 95;
        }
    }

    /**
     * interrupts every other call
     */
    public static class EveryOtherInterrupt implements InterruptListener {
        protected boolean interrupting = false;

        @Override
        public boolean interrupt(Key key) {
            return interrupting;
        }

        @Override
        public void processedInterrupt(boolean interrupt) {
            interrupting = !interrupting;
        }
    }

    /**
     * interrupt every other fi key, otherwise don't interrupt
     */
    public static class FiEveryOtherInterrupt extends EveryOtherInterrupt {
        public FiEveryOtherInterrupt() {
            // initialize to true first
            processedInterrupt(false);
        }

        @Override
        public boolean interrupt(Key key) {
            if (key != null && key.getColumnFamily().toString().startsWith("fi" + Constants.NULL)) {
                return super.interrupt(key);
            }

            return false;
        }
    }

    public static AccumuloClient getClient(InMemoryInstance i, String user, byte[] pass, TEARDOWN teardown, INTERRUPT interrupt)
                    throws AccumuloSecurityException {
        return new RebuildingAccumuloClient(user, i, teardown, interrupt);
    }

    public static AccumuloClient getClient(InMemoryInstance i, String user, ByteBuffer pass, TEARDOWN teardown, INTERRUPT interrupt)
                    throws AccumuloSecurityException {
        return new RebuildingAccumuloClient(user, i, teardown, interrupt);
    }

    public static AccumuloClient getClient(InMemoryInstance i, String user, CharSequence pass, TEARDOWN teardown, INTERRUPT interrupt)
                    throws AccumuloSecurityException {
        return new RebuildingAccumuloClient(user, i, teardown, interrupt);
    }

    public static AccumuloClient getClient(InMemoryInstance i, String principal, AuthenticationToken token, TEARDOWN teardown, INTERRUPT interrupt)
                    throws AccumuloSecurityException {
        return new RebuildingAccumuloClient(principal, i, teardown, interrupt);
    }

    public interface TeardownListener {
        boolean teardown();

        boolean checkConsistency();
    }

    public static class RebuildingIterator implements Iterator<Map.Entry<Key,Value>> {
        private final InMemoryScannerBase baseScanner;
        private Iterator<Map.Entry<Key,Value>> delegate;
        private final ScannerRebuilder scanner;
        private final TeardownListener teardown;
        private final InterruptListener interruptListener;
        private Map.Entry<Key,Value> next = null;
        private Map.Entry<Key,Value> lastKey = null;
        private final KryoDocumentDeserializer deserializer = new KryoDocumentDeserializer();
        private boolean initialized = false;

        public RebuildingIterator(InMemoryScannerBase baseScanner, ScannerRebuilder scanner, TeardownListener teardown, InterruptListener interruptListener) {
            this.baseScanner = baseScanner;
            this.scanner = scanner;
            this.teardown = teardown;
            this.interruptListener = interruptListener;
            init();
        }

        private void init() {
            // create the interruptIterator and add it to the base scanner
            InterruptIterator interruptIterator = new InterruptIterator();
            interruptIterator.setInterruptListener(interruptListener);

            // add it to the baseScanner injected list so that this iterator is not torn down and losing state with
            // every interrupt or rebuild
            baseScanner.addInjectedIterator(interruptIterator);
        }

        @Override
        public boolean hasNext() {
            if (!initialized) {
                findNext();
                initialized = true;
            }

            return next != null;
        }

        @Override
        public Map.Entry<Key,Value> next() {
            if (!initialized) {
                findNext();
                initialized = true;
            }

            lastKey = next;
            findNext();
            return lastKey;
        }

        private void findNext() {
            boolean interrupted = false;
            int interruptCount = 0;
            // track if the iterator has already been rebuilt or not, do not rebuild on the same key more than once
            boolean rebuilt = false;
            do {
                try {
                    if (delegate == null) {
                        // build initial iterator. It can be interrupted since it calls seek/next under the covers to prepare the first key
                        delegate = baseScanner.iterator();
                    }

                    if (interrupted) {
                        Key last = lastKey != null ? lastKey.getKey() : null;
                        if (!rebuilt) {
                            delegate = scanner.rebuild(last);
                            rebuilt = true;
                        } else {
                            // cal with a null last key to prevent an update to the ranges
                            delegate = scanner.rebuild(null);
                        }
                    }

                    if (lastKey != null && teardown.teardown()) {
                        boolean hasNext = delegate.hasNext();
                        Map.Entry<Key,Value> next = (hasNext ? delegate.next() : null);
                        if (hasNext && (next == null)) {
                            throw new RuntimeException("Pre teardown: If hasNext() is true, next() must not return null. interrupted: " + interruptCount);
                        }

                        if (!rebuilt) {
                            delegate = scanner.rebuild(lastKey.getKey());
                            rebuilt = true;
                        } else {
                            // was already rebuilt once with this key, just re-create the iterator
                            delegate = scanner.rebuild(null);
                        }

                        boolean rebuildHasNext = delegate.hasNext();
                        Map.Entry<Key,Value> rebuildNext = (rebuildHasNext ? delegate.next() : null);
                        if (rebuildHasNext && (rebuildNext == null)) {
                            throw new RuntimeException("After rebuild: If hasNext() is true, next() must not return null. interrupted: " + interruptCount);
                        }

                        if (hasNext != rebuildHasNext) {
                            // the only known scenario where this is ok is when the rebuild causes us to have a "FinalDocument" where previously we did not
                            // have/need one
                            if (teardown.checkConsistency() && (hasNext || !FinalDocumentTrackingIterator.isFinalDocumentKey(rebuildNext.getKey()))) {
                                throw new RuntimeException("Unexpected change in top key: hasNext is no longer " + hasNext + " interrupted: " + interruptCount);
                            }
                        } else if (hasNext) {
                            if (teardown.checkConsistency()) {
                                // if we are dealing with a final document, then both pre-build and post-build must be returning a final document
                                // otherwise the keys need to be identical save for the timestamp
                                boolean nextFinal = FinalDocumentTrackingIterator.isFinalDocumentKey(next.getKey());
                                boolean rebuildNextFinal = FinalDocumentTrackingIterator.isFinalDocumentKey(rebuildNext.getKey());
                                if (nextFinal || rebuildNextFinal) {
                                    if (nextFinal != rebuildNextFinal) {
                                        throw new RuntimeException("Unexpected change in top key: expected " + next + " BUT GOT " + rebuildNext
                                                        + " interrupted: " + interruptCount);
                                    }
                                } else if (!next.getKey().equals(rebuildNext.getKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {
                                    final Document rebuildDocument = deserializer.apply(rebuildNext).getValue();
                                    final Document lastDocument = deserializer.apply(lastKey).getValue();
                                    // if the keys don't match but are returning the previous document and using a new key this is okay
                                    if (!lastDocument.get("RECORD_ID").getData().toString().equals(rebuildDocument.get("RECORD_ID").getData().toString())
                                                    && !lastKey.getKey().equals(rebuildNext.getKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {
                                        throw new RuntimeException("Unexpected change in top key: expected " + next + " BUT GOT " + rebuildNext);
                                    }
                                }
                            }
                        }

                        this.next = rebuildNext;
                    } else {
                        this.next = (delegate.hasNext() ? delegate.next() : null);
                        // only clear interrupts flag when not dealing with rebuilds
                        interruptListener.processedInterrupt(false);
                    }
                    // reset interrupted flag
                    interrupted = false;
                } catch (IterationInterruptedException e) {
                    interrupted = true;
                    interruptListener.processedInterrupt(true);
                    interruptCount++;
                }
            } while (interrupted);
        }

        @Override
        public void remove() {
            delegate.remove();
        }

        @Override
        public void forEachRemaining(Consumer<? super Map.Entry<Key,Value>> action) {
            delegate.forEachRemaining(action);
        }
    }

    public static class RandomTeardown implements TeardownListener {
        private final Random random = new Random();

        @Override
        public boolean teardown() {
            return (random.nextBoolean());
        }

        @Override
        public boolean checkConsistency() {
            return true;
        }
    }

    public static class RandomTeardownWithoutConsistency extends RandomTeardown {
        @Override
        public boolean checkConsistency() {
            return false;
        }
    }

    public static class EveryOtherTeardown implements TeardownListener {
        private transient boolean teardown = false;

        @Override
        public boolean teardown() {
            teardown = !teardown;
            return (teardown);
        }

        @Override
        public boolean checkConsistency() {
            return true;
        }
    }

    public static class EveryOtherTeardownWithoutConsistency extends EveryOtherTeardown {
        @Override
        public boolean checkConsistency() {
            return false;
        }
    }

    public static class AlwaysTeardown implements TeardownListener {
        @Override
        public boolean teardown() {
            return true;
        }

        @Override
        public boolean checkConsistency() {
            return true;
        }
    }

    public static class AlwaysTeardownWithoutConsistency extends AlwaysTeardown {
        @Override
        public boolean checkConsistency() {
            return false;
        }
    }

    public static class NeverTeardown implements TeardownListener {
        @Override
        public boolean teardown() {
            return false;
        }

        @Override
        public boolean checkConsistency() {
            return true;
        }
    }

    public static class RebuildingScanner extends DelegatingScannerBase implements Scanner {
        private final TEARDOWN teardown;
        private final INTERRUPT interrupt;

        public RebuildingScanner(InMemoryScanner delegate, TEARDOWN teardown, INTERRUPT interrupt) {
            super(delegate);
            this.teardown = teardown;
            this.interrupt = interrupt;
        }

        @Override
        public Iterator<Map.Entry<Key,Value>> iterator() {
            try {
                return new RebuildingIterator((InMemoryScannerBase) delegate, ((InMemoryScanner) delegate).clone(), teardown.instance(), interrupt.instance());
            } catch (Exception e) {
                throw new RuntimeException("Misconfigured teardown listener class most likely", e);
            }
        }

        @Override
        public ConsistencyLevel getConsistencyLevel() {
            return ConsistencyLevel.IMMEDIATE;
        }

        @Override
        public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {

        }

        @Override
        public synchronized void setExecutionHints(Map<String,String> hints) {
            // no-op
        }

        @Override
        public void setRange(Range range) {
            ((InMemoryScanner) delegate).setRange(range);
        }

        @Override
        public Range getRange() {
            return ((InMemoryScanner) delegate).getRange();
        }

        @Override
        public void setBatchSize(int size) {
            ((InMemoryScanner) delegate).setBatchSize(size);
        }

        @Override
        public int getBatchSize() {
            return ((InMemoryScanner) delegate).getBatchSize();
        }

        @Override
        public void enableIsolation() {
            ((InMemoryScanner) delegate).enableIsolation();
        }

        @Override
        public void disableIsolation() {
            ((InMemoryScanner) delegate).disableIsolation();
        }

        @Override
        public long getReadaheadThreshold() {
            return ((InMemoryScanner) delegate).getReadaheadThreshold();
        }

        @Override
        public void setReadaheadThreshold(long batches) {
            ((InMemoryScanner) delegate).setReadaheadThreshold(batches);
        }
    }

    public static class RebuildingBatchScanner extends DelegatingScannerBase implements BatchScanner {
        private final TEARDOWN teardown;
        private final INTERRUPT interrupt;

        public RebuildingBatchScanner(InMemoryBatchScanner delegate, TEARDOWN teardown, INTERRUPT interrupt) {
            super(delegate);
            this.teardown = teardown;
            this.interrupt = interrupt;
        }

        @Override
        public Iterator<Map.Entry<Key,Value>> iterator() {
            try {
                return new RebuildingIterator((InMemoryScannerBase) delegate, ((InMemoryBatchScanner) delegate).clone(), teardown.instance(),
                                interrupt.instance());
            } catch (Exception e) {
                throw new RuntimeException("Misconfigured teardown listener class most likely", e);
            }
        }

        @Override
        public ConsistencyLevel getConsistencyLevel() {
            return ConsistencyLevel.IMMEDIATE;
        }

        @Override
        public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {

        }

        @Override
        public synchronized void setExecutionHints(Map<String,String> hints) {
            // no-op
        }

        @Override
        public void setRanges(Collection<Range> ranges) {
            ((InMemoryBatchScanner) delegate).setRanges(ranges);
        }
    }

    public static class RebuildingAccumuloClient extends InMemoryAccumuloClient {
        private final TEARDOWN teardown;
        private final INTERRUPT interrupt;

        public RebuildingAccumuloClient(String user, InMemoryInstance instance, TEARDOWN teardown, INTERRUPT interrupt) throws AccumuloSecurityException {
            super(user, instance);
            this.teardown = teardown;
            this.interrupt = interrupt;
        }

        @Override
        public BatchScanner createBatchScanner(String s, Authorizations authorizations, int i) throws TableNotFoundException {
            return new RebuildingBatchScanner((InMemoryBatchScanner) (super.createBatchScanner(s, authorizations, i)), teardown, interrupt);
        }

        @Override
        public BatchScanner createBatchScanner(String s, Authorizations authorizations) throws TableNotFoundException {
            return new RebuildingBatchScanner((InMemoryBatchScanner) (super.createBatchScanner(s, authorizations)), teardown, interrupt);
        }

        @Override
        public BatchScanner createBatchScanner(String s) throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
            return new RebuildingBatchScanner((InMemoryBatchScanner) (super.createBatchScanner(s)), teardown, interrupt);
        }

        @Override
        public Scanner createScanner(String s, Authorizations authorizations) throws TableNotFoundException {
            return new RebuildingScanner((InMemoryScanner) (super.createScanner(s, authorizations)), teardown, interrupt);
        }

        @Override
        public Scanner createScanner(String s) throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
            return new RebuildingScanner((InMemoryScanner) (super.createScanner(s)), teardown, interrupt);
        }
    }

    public static class DelegatingScannerBase implements ScannerBase {
        protected final ScannerBase delegate;

        public DelegatingScannerBase(ScannerBase delegate) {
            this.delegate = delegate;
        }

        @Override
        public void addScanIterator(IteratorSetting cfg) {
            delegate.addScanIterator(cfg);
        }

        @Override
        public void removeScanIterator(String iteratorName) {
            delegate.removeScanIterator(iteratorName);
        }

        @Override
        public void updateScanIteratorOption(String iteratorName, String key, String value) {
            delegate.updateScanIteratorOption(iteratorName, key, value);
        }

        @Override
        public void fetchColumnFamily(Text col) {
            delegate.fetchColumnFamily(col);
        }

        @Override
        public void fetchColumn(Text colFam, Text colQualifier) {
            delegate.fetchColumn(colFam, colQualifier);
        }

        @Override
        public void fetchColumn(IteratorSetting.Column column) {
            delegate.fetchColumn(column);
        }

        @Override
        public void clearColumns() {
            delegate.clearColumns();
        }

        @Override
        public void clearScanIterators() {
            delegate.clearScanIterators();
        }

        @Override
        public Iterator<Map.Entry<Key,Value>> iterator() {
            return delegate.iterator();
        }

        @Override
        public void setTimeout(long timeOut, TimeUnit timeUnit) {
            delegate.setTimeout(timeOut, timeUnit);
        }

        @Override
        public long getTimeout(TimeUnit timeUnit) {
            return delegate.getTimeout(timeUnit);
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        public Authorizations getAuthorizations() {
            return delegate.getAuthorizations();
        }

        @Override
        public void setSamplerConfiguration(SamplerConfiguration samplerConfig) {
            delegate.setSamplerConfiguration(samplerConfig);
        }

        @Override
        public SamplerConfiguration getSamplerConfiguration() {
            return delegate.getSamplerConfiguration();
        }

        @Override
        public void clearSamplerConfiguration() {
            delegate.clearSamplerConfiguration();
        }

        @Override
        public void setBatchTimeout(long timeOut, TimeUnit timeUnit) {
            delegate.setBatchTimeout(timeOut, timeUnit);
        }

        @Override
        public long getBatchTimeout(TimeUnit timeUnit) {
            return delegate.getBatchTimeout(timeUnit);
        }

        @Override
        public void setClassLoaderContext(String classLoaderContext) {
            delegate.setClassLoaderContext(classLoaderContext);
        }

        @Override
        public void clearClassLoaderContext() {
            delegate.clearClassLoaderContext();
        }

        @Override
        public String getClassLoaderContext() {
            return delegate.getClassLoaderContext();
        }

        @Override
        public ConsistencyLevel getConsistencyLevel() {
            return ConsistencyLevel.IMMEDIATE;
        }

        @Override
        public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {

        }

        @Override
        public void forEach(Consumer<? super Map.Entry<Key,Value>> action) {
            delegate.forEach(action);
        }

        @Override
        public Spliterator<Map.Entry<Key,Value>> spliterator() {
            return delegate.spliterator();
        }
    }

}
