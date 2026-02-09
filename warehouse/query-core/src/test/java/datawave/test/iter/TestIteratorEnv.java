package datawave.test.iter;

import java.io.IOException;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.crypto.CryptoFactoryLoader;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.rfile.RFileOperations;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class TestIteratorEnv implements IteratorEnvironment {

    private IteratorScope scope;
    private final AccumuloConfiguration conf;

    public TestIteratorEnv() {
        this.conf = DefaultConfiguration.getInstance();
    }

    public void setScope(IteratorScope scope) {
        this.scope = scope;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> reserveMapFileReader(String mapFileName) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        CryptoService cs = CryptoFactoryLoader.getServiceForClient(CryptoEnvironment.Scope.TABLE, DefaultConfiguration.getInstance().getAllCryptoProperties());
        return RFileOperations.getInstance().newReaderBuilder().forFile(mapFileName, fs, conf, cs).withTableConfiguration(DefaultConfiguration.getInstance())
                        .seekToBeginning().build();
    }

    @Override
    public IteratorScope getIteratorScope() {
        return scope;
    }

    @Override
    public boolean isFullMajorCompaction() {
        return scope.equals(IteratorScope.majc);
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

    @Override
    public boolean isUserCompaction() {
        return false;
    }

    @Override
    public ServiceEnvironment getServiceEnv() {
        return null;
    }

    @Override
    public PluginEnvironment getPluginEnv() {
        return new TestPluginEnv();
    }

    @Override
    public TableId getTableId() {
        return null;
    }

    @Override
    public AccumuloConfiguration getConfig() {
        return conf;
    }
}
