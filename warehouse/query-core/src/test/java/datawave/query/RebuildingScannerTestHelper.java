package datawave.query;

import datawave.accumulo.inmemory.InMemoryBatchScanner;
import datawave.accumulo.inmemory.InMemoryConnector;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.accumulo.inmemory.InMemoryScanner;
import datawave.accumulo.inmemory.ScannerRebuilder;
import datawave.query.iterator.profile.FinalDocumentTrackingIterator;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.NamespaceOperations;
import org.apache.accumulo.core.client.admin.ReplicationOperations;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.Text;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * This helper provides support for testing the teardown of iterators at randomly. Simply use the getConnector methods and all scanners created will contain an
 * iterator that will force the stack to be torn down.
 */
public class RebuildingScannerTestHelper {
    
    public enum TEARDOWN {
        NEVER(NeverTeardown.class), ALWAYS(AlwaysTeardown.class), RANDOM(RandomTeardown.class), EVERY_OTHER(EveryOtherTeardown.class), ALWAYS_SANS_CONSISTENCY(
                        AlwaysTeardownWithoutConsistency.class), RANDOM_SANS_CONSISTENCY(RandomTeardownWithoutConsistency.class), EVERY_OTHER_SANS_CONSISTENCY(
                        EveryOtherTeardownWithoutConsistency.class);
        
        private Class<? extends TeardownListener> tclass;
        
        TEARDOWN(Class<? extends TeardownListener> tclass) {
            this.tclass = tclass;
        }
        
        public TeardownListener instance() {
            try {
                return tclass.newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    public static Connector getConnector(InMemoryInstance i, String user, byte[] pass, TEARDOWN teardown) throws AccumuloException, AccumuloSecurityException {
        return new RebuildingConnector((InMemoryConnector) (i.getConnector(user, new PasswordToken(pass))), teardown);
    }
    
    public static Connector getConnector(InMemoryInstance i, String user, ByteBuffer pass, TEARDOWN teardown) throws AccumuloException,
                    AccumuloSecurityException {
        return new RebuildingConnector((InMemoryConnector) (i.getConnector(user, ByteBufferUtil.toBytes(pass))), teardown);
    }
    
    public static Connector getConnector(InMemoryInstance i, String user, CharSequence pass, TEARDOWN teardown) throws AccumuloException,
                    AccumuloSecurityException {
        return new RebuildingConnector((InMemoryConnector) (i.getConnector(user, TextUtil.getBytes(new Text(pass.toString())))), teardown);
    }
    
    public static Connector getConnector(InMemoryInstance i, String principal, AuthenticationToken token, TEARDOWN teardown) throws AccumuloException,
                    AccumuloSecurityException {
        return new RebuildingConnector((InMemoryConnector) (i.getConnector(principal, token)), teardown);
    }
    
    public interface TeardownListener {
        boolean teardown();
        
        boolean checkConsistency();
    }
    
    public static class RebuildingIterator implements Iterator<Map.Entry<Key,Value>> {
        private Iterator<Map.Entry<Key,Value>> delegate;
        private final ScannerRebuilder scanner;
        private final TeardownListener teardown;
        private Map.Entry<Key,Value> next = null;
        private Map.Entry<Key,Value> lastKey = null;
        
        public RebuildingIterator(Iterator<Map.Entry<Key,Value>> delegate, ScannerRebuilder scanner, TeardownListener teardown) {
            this.delegate = delegate;
            this.scanner = scanner;
            this.teardown = teardown;
            findNext();
        }
        
        @Override
        public boolean hasNext() {
            return next != null;
        }
        
        @Override
        public Map.Entry<Key,Value> next() {
            lastKey = next;
            findNext();
            return lastKey;
        }
        
        private void findNext() {
            if (lastKey != null && teardown.teardown()) {
                boolean hasNext = delegate.hasNext();
                Map.Entry<Key,Value> next = (hasNext ? delegate.next() : null);
                if ((hasNext == false) != (next == null)) {
                    throw new RuntimeException("Has next does not equate to next being null: " + hasNext + " vs " + next);
                }
                
                delegate = scanner.rebuild(lastKey.getKey());
                
                boolean rebuildHasNext = delegate.hasNext();
                Map.Entry<Key,Value> rebuildNext = (rebuildHasNext ? delegate.next() : null);
                if ((rebuildHasNext == false) != (rebuildNext == null)) {
                    throw new RuntimeException("Has next does not equate to next being null: " + rebuildHasNext + " vs " + rebuildNext);
                }
                
                if (hasNext != rebuildHasNext) {
                    // the only known scenario where this is ok is when the rebuild causes us to have a "FinalDocument" where previously we did not have/need
                    // one
                    if (teardown.checkConsistency() && (hasNext || !FinalDocumentTrackingIterator.isFinalDocumentKey(rebuildNext.getKey()))) {
                        throw new RuntimeException("Unexpected change in top key: hasNext is no longer " + hasNext);
                    }
                } else if (hasNext) {
                    if (teardown.checkConsistency()) {
                        // if we are dealing with a final document, then both prebuild and postbuild must be returning a final document
                        // otherwise the keys need to be identical save for the timestamp
                        boolean nextFinal = FinalDocumentTrackingIterator.isFinalDocumentKey(next.getKey());
                        boolean rebuildNextFinal = FinalDocumentTrackingIterator.isFinalDocumentKey(rebuildNext.getKey());
                        if (nextFinal || rebuildNextFinal) {
                            if (nextFinal != rebuildNextFinal) {
                                throw new RuntimeException("Unexpected change in top key: expected " + next + " BUT GOT " + rebuildNext);
                            }
                        } else if (!next.getKey().equals(rebuildNext.getKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {
                            throw new RuntimeException("Unexpected change in top key: expected " + next + " BUT GOT " + rebuildNext);
                        }
                    }
                }
                
                this.next = rebuildNext;
            } else {
                this.next = (delegate.hasNext() ? delegate.next() : null);
            }
            
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
        
        public RebuildingScanner(InMemoryScanner delegate, TEARDOWN teardown) {
            super(delegate);
            this.teardown = teardown;
        }
        
        @Override
        public Iterator<Map.Entry<Key,Value>> iterator() {
            try {
                return new RebuildingIterator(delegate.iterator(), ((InMemoryScanner) delegate).clone(), teardown.instance());
            } catch (Exception e) {
                throw new RuntimeException("Misconfigured teardown listener class most likely", e);
            }
        }
        
        @Override
        public void setTimeOut(int timeOut) {
            ((InMemoryScanner) delegate).setTimeOut(timeOut);
        }
        
        @Override
        public int getTimeOut() {
            return ((InMemoryScanner) delegate).getTimeOut();
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
        
        public RebuildingBatchScanner(InMemoryBatchScanner delegate, TEARDOWN teardown) {
            super(delegate);
            this.teardown = teardown;
        }
        
        @Override
        public Iterator<Map.Entry<Key,Value>> iterator() {
            try {
                return new RebuildingIterator(delegate.iterator(), ((InMemoryBatchScanner) delegate).clone(), teardown.instance());
            } catch (Exception e) {
                throw new RuntimeException("Misconfigured teardown listener class most likely", e);
            }
        }
        
        @Override
        public void setRanges(Collection<Range> ranges) {
            ((InMemoryBatchScanner) delegate).setRanges(ranges);
        }
    }
    
    public static class RebuildingConnector extends Connector {
        private final InMemoryConnector delegate;
        private final TEARDOWN teardown;
        
        public RebuildingConnector(InMemoryConnector delegate, TEARDOWN teardown) {
            this.delegate = delegate;
            this.teardown = teardown;
        }
        
        @Override
        public BatchScanner createBatchScanner(String s, Authorizations authorizations, int i) throws TableNotFoundException {
            return new RebuildingBatchScanner((InMemoryBatchScanner) (delegate.createBatchScanner(s, authorizations, i)), teardown);
        }
        
        @Override
        @Deprecated
        public BatchDeleter createBatchDeleter(String s, Authorizations authorizations, int i, long l, long l1, int i1) throws TableNotFoundException {
            return delegate.createBatchDeleter(s, authorizations, i, l, l1, i1);
        }
        
        @Override
        public BatchDeleter createBatchDeleter(String s, Authorizations authorizations, int i, BatchWriterConfig batchWriterConfig)
                        throws TableNotFoundException {
            return delegate.createBatchDeleter(s, authorizations, i, batchWriterConfig);
        }
        
        @Override
        @Deprecated
        public BatchWriter createBatchWriter(String s, long l, long l1, int i) throws TableNotFoundException {
            return delegate.createBatchWriter(s, l, l1, i);
        }
        
        @Override
        public BatchWriter createBatchWriter(String s, BatchWriterConfig batchWriterConfig) throws TableNotFoundException {
            return delegate.createBatchWriter(s, batchWriterConfig);
        }
        
        @Override
        @Deprecated
        public MultiTableBatchWriter createMultiTableBatchWriter(long l, long l1, int i) {
            return delegate.createMultiTableBatchWriter(l, l1, i);
        }
        
        @Override
        public MultiTableBatchWriter createMultiTableBatchWriter(BatchWriterConfig batchWriterConfig) {
            return delegate.createMultiTableBatchWriter(batchWriterConfig);
        }
        
        @Override
        public Scanner createScanner(String s, Authorizations authorizations) throws TableNotFoundException {
            return new RebuildingScanner((InMemoryScanner) (delegate.createScanner(s, authorizations)), teardown);
        }
        
        @Override
        public ConditionalWriter createConditionalWriter(String s, ConditionalWriterConfig conditionalWriterConfig) throws TableNotFoundException {
            return delegate.createConditionalWriter(s, conditionalWriterConfig);
        }
        
        @Override
        public Instance getInstance() {
            return delegate.getInstance();
        }
        
        @Override
        public String whoami() {
            return delegate.whoami();
        }
        
        @Override
        public TableOperations tableOperations() {
            return delegate.tableOperations();
        }
        
        @Override
        public NamespaceOperations namespaceOperations() {
            return delegate.namespaceOperations();
        }
        
        @Override
        public SecurityOperations securityOperations() {
            return delegate.securityOperations();
        }
        
        @Override
        public InstanceOperations instanceOperations() {
            return delegate.instanceOperations();
        }
        
        @Override
        public ReplicationOperations replicationOperations() {
            return delegate.replicationOperations();
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
        public void fetchColumn(Text colFam, Text colQual) {
            delegate.fetchColumn(colFam, colQual);
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
        public void forEach(Consumer<? super Map.Entry<Key,Value>> action) {
            delegate.forEach(action);
        }
        
        @Override
        public Spliterator<Map.Entry<Key,Value>> spliterator() {
            return delegate.spliterator();
        }
    }
    
}
