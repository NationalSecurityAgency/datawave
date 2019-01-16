package datawave.webservice.common.connection;

import java.util.Collection;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.Range;

/**
 * A simple wrapper around a {@link BatchScanner} that overrides the methods that configure iterators.
 */
public class BatchScannerDelegate extends ScannerBaseDelegate implements BatchScanner {
    public BatchScannerDelegate(BatchScanner delegate) {
        super(delegate);
    }
    
    @Override
    public final void setRanges(Collection<Range> ranges) {
        ((BatchScanner) delegate).setRanges(ranges);
    }
}
