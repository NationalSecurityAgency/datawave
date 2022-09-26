package datawave.iterators.filter.ageoff;

import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;

import java.io.IOException;

// todo move to a shared test resource
public class StubbedIteratorEnvironment implements IteratorEnvironment {
    @Override
    public SortedKeyValueIterator<Key,Value> reserveMapFileReader(String s) throws IOException {
        return null;
    }
    
    @Override
    public AccumuloConfiguration getConfig() {
        return null;
    }
    
    @Override
    public IteratorUtil.IteratorScope getIteratorScope() {
        return null;
    }
    
    @Override
    public boolean isFullMajorCompaction() {
        return false;
    }
    
    @Override
    public void registerSideChannel(SortedKeyValueIterator<Key,Value> sortedKeyValueIterator) {
        
    }
    
    @Override
    public Authorizations getAuthorizations() {
        return null;
    }
    
    @Override
    public IteratorEnvironment cloneWithSamplingEnabled() {
        return null;
    }
    
    @Override
    public boolean isSamplingEnabled() {
        return false;
    }
    
    @Override
    public SamplerConfiguration getSamplerConfiguration() {
        return null;
    }
}
