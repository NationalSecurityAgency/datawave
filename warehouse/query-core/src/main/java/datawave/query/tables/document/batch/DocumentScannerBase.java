package datawave.query.tables.document.batch;

import datawave.query.tables.serialization.SerializedDocumentIfc;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.clientImpl.ScannerOptions;
import org.apache.accumulo.core.data.Range;

import java.util.ArrayList;
import java.util.Iterator;

public abstract class DocumentScannerBase extends ScannerOptions implements BatchScanner {


    protected ArrayList<Range> ranges = null;

    public abstract Iterator<SerializedDocumentIfc> getDocumentIterator();


    public void setRange(Range next) {
        this.ranges = new ArrayList<>();
        this.ranges.add(next);
    }
}
