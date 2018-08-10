package datawave.query.testframework;

import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * Basic class used for testing when a skeleton {@link RawDataManager} is needed. All methods are implemented without any logic. Test classes should extend this
 * class and implement any needed functionality.
 */
abstract class BaseTestManager extends AbstractDataManager {
    
    private final List<String> baseHeaders;
    
    BaseTestManager(final String keyField, final String shardField, final List<String> hdrs) {
        super(keyField, shardField);
        this.baseHeaders = hdrs;
    }
    
    @Override
    public List<String> getHeaders() {
        return baseHeaders;
    }
    
    @Override
    public void addTestData(URI file, String datatype, Set<String> indexes) throws IOException {
        
    }
    
    @Override
    public Date[] getRandomStartEndDate() {
        return new Date[0];
    }
    
    @Override
    public Date[] getShardStartEndDate() {
        return new Date[0];
    }
}
