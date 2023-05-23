package datawave.query.tables.document.batch;

import datawave.query.DocumentSerialization;
import datawave.query.tables.serialization.SerializedDocumentIfc;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;

public class InMemoryDocumentScannerImpl extends DocumentScannerBase {


    private static final Logger log = LoggerFactory.getLogger(InMemoryDocumentScannerImpl.class);
    private final TableId tableId;
    private final String tableName;
    private final int numThreads;
    private final ClientContext context;
    private final Authorizations authorizations;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final DocumentSerialization.ReturnType returnType;
    private final boolean docRawFields;
    private final BatchScanner scanner;
    private int maxTabletsPerRequest=0;

    private ArrayList<Range> ranges = null;
    private Collection<IteratorSetting> iteratorSettings = new ArrayList<>();

    public InMemoryDocumentScannerImpl(ClientContext context, TableId tableId,
                                       String tableName, Authorizations authorizations, int numQueryThreads, DocumentSerialization.ReturnType ret, boolean docRawFields) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
        checkArgument(context != null, "context is null");
        checkArgument(tableId != null, "tableId is null");
        checkArgument(authorizations != null, "authorizations is null");
        this.context = context;
        this.authorizations = authorizations;
        this.tableId = tableId;
        this.tableName = tableName;
        this.numThreads = numQueryThreads;
        this.returnType = ret;
        this.docRawFields=docRawFields;
        this.scanner = context.createBatchScanner(tableName, authorizations,numQueryThreads);
    }

    @Override
    public void close() {
        if (!closed.get()){
            scanner.close();
            closed.set(true);
        }

    }

    @Override
    public Authorizations getAuthorizations() {
        return authorizations;
    }

    @Override
    public void setRanges(Collection<Range> ranges) {
        if (ranges == null || ranges.isEmpty()) {
            throw new IllegalArgumentException("ranges must be non null and contain at least 1 range");
        }

        if (closed.get()) {
            throw new IllegalStateException("batch reader closed");
        }

        this.ranges = new ArrayList<>(ranges);
    }

    @Override
    public Iterator<Map.Entry<Key, Value>> iterator() {
        throw new UnsupportedOperationException("use getDocumentIterator");
    }

    public synchronized void addScanIterator(IteratorSetting si) {
        this.iteratorSettings.add(si);
        super.addScanIterator(si);
    }
    public Iterator<SerializedDocumentIfc> getDocumentIterator() {
        if (ranges == null) {
            throw new IllegalStateException("ranges not set");
        }

        if (closed.get()) {
            throw new IllegalStateException("batch reader closed");
        }
        scanner.setRanges(ranges);
        iteratorSettings.forEach( setting -> scanner.addScanIterator(setting));
        return scanner.stream().map(
                keyValue -> {
                    return DocumentKeyConversion.getDocument(returnType, docRawFields, keyValue);
                }
        ).iterator();

    }

    public void setRange(Range next) {
        this.ranges = new ArrayList<>();
        this.ranges.add(next);
    }
}