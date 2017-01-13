package nsa.datawave.webservice.common.connection;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Range;

/**
 * A simple wrapper around a {@link Scanner} that overrides the methods that configure iterators.
 */
public class ScannerDelegate extends ScannerBaseDelegate implements Scanner {
    public ScannerDelegate(Scanner delegate) {
        super(delegate);
    }
    
    @Override
    @Deprecated
    @SuppressWarnings("deprecation")
    public final void setTimeOut(int timeOut) {
        ((Scanner) delegate).setTimeOut(timeOut);
    }
    
    @Override
    @Deprecated
    @SuppressWarnings("deprecation")
    public final int getTimeOut() {
        return ((Scanner) delegate).getTimeOut();
    }
    
    @Override
    public final void setRange(Range range) {
        ((Scanner) delegate).setRange(range);
    }
    
    @Override
    public final Range getRange() {
        return ((Scanner) delegate).getRange();
    }
    
    @Override
    public final void setBatchSize(int size) {
        ((Scanner) delegate).setBatchSize(size);
    }
    
    @Override
    public final int getBatchSize() {
        return ((Scanner) delegate).getBatchSize();
    }
    
    @Override
    public final void enableIsolation() {
        ((Scanner) delegate).enableIsolation();
    }
    
    @Override
    public final void disableIsolation() {
        ((Scanner) delegate).disableIsolation();
    }
    
    @Override
    public final long getReadaheadThreshold() {
        return ((Scanner) delegate).getReadaheadThreshold();
    }
    
    @Override
    public final void setReadaheadThreshold(long batches) {
        ((Scanner) delegate).setReadaheadThreshold(batches);
    }
}
