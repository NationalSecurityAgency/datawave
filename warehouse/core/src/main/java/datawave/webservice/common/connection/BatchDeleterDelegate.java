package datawave.webservice.common.connection;

import java.util.Collection;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;

/**
 * A simple wrapper around a {@link BatchDeleter} that overrides the methods that configure iterators.
 */
public class BatchDeleterDelegate extends ScannerBaseDelegate implements BatchDeleter {
    
    public BatchDeleterDelegate(BatchDeleter delegate) {
        super(delegate);
    }
    
    @Override
    public final void delete() throws MutationsRejectedException, TableNotFoundException {
        ((BatchDeleter) delegate).delete();
    }
    
    @Override
    public final void setRanges(Collection<Range> ranges) {
        ((BatchDeleter) delegate).setRanges(ranges);
    }
}
