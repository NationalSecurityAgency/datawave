package datawave.iterators.test;

import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;

import java.io.IOException;

/**
 * Provides a stub implementation of IteratorEnvironment that's methods can be overridden as needed within a unit test
 */
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
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void registerSideChannel(SortedKeyValueIterator<Key,Value> sortedKeyValueIterator) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Authorizations getAuthorizations() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public IteratorEnvironment cloneWithSamplingEnabled() {
        throw new SampleNotPresentException();
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
