package nsa.datawave.mr.bulk;

import java.io.IOException;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class BulkIteratorEnvironment implements IteratorEnvironment {
    
    AccumuloConfiguration conf;
    
    public BulkIteratorEnvironment(AccumuloConfiguration conf) {
        this.conf = conf;
    }
    
    public BulkIteratorEnvironment() {
        this.conf = AccumuloConfiguration.getDefaultConfiguration();
    }
    
    @Override
    public AccumuloConfiguration getConfig() {
        return conf;
    }
    
    @Override
    public IteratorScope getIteratorScope() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public boolean isFullMajorCompaction() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void registerSideChannel(SortedKeyValueIterator<Key,Value> iter) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> reserveMapFileReader(String mapFileName) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }
    
}
