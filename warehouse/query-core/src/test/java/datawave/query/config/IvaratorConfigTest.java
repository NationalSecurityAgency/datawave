package datawave.query.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

import datawave.query.iterator.QueryOptions;

class IvaratorConfigTest {

    @Test
    public void testSerializeDeserializeEquivalence() throws JsonProcessingException {
        IvaratorConfig conf1 = new IvaratorConfig();
        conf1.setIvaratorFstHdfsBaseURIs("file://tmp/ivarator");
        conf1.setIvaratorCacheBufferSize(1024);
        conf1.setIvaratorCacheScanPersistThreshold(5000L);
        conf1.setIvaratorCacheScanTimeout(10000L);
        conf1.setMaxFieldIndexRangeSplit(50);
        conf1.setIvaratorMaxOpenFiles(200);
        conf1.setIvaratorNumRetries(3);
        conf1.setIvaratorPersistVerify(true);
        conf1.setIvaratorPersistVerifyCount(10);
        conf1.setMaxIvaratorSources(1000L);
        conf1.setMaxIvaratorSourceWait(15000L);
        conf1.setMaxIvaratorResults(1000000L);

        String serialized = IvaratorConfig.toJson(conf1);
        IvaratorConfig deserialized = IvaratorConfig.fromJson(serialized);
        String reserialized = IvaratorConfig.toJson(deserialized);
        IvaratorConfig redeserialized = IvaratorConfig.fromJson(reserialized);

        Assertions.assertEquals(serialized, reserialized);
        Assertions.assertEquals(deserialized, redeserialized);

    }

    @Test
    public void testSettersGetters() throws JsonProcessingException {

        IvaratorConfig conf1 = new IvaratorConfig();
        conf1.setIvaratorFstHdfsBaseURIs("file://tmp/ivarator");
        conf1.setIvaratorCacheBufferSize(1024);
        conf1.setIvaratorCacheScanPersistThreshold(5000L);
        conf1.setIvaratorCacheScanTimeout(10000L);
        conf1.setMaxFieldIndexRangeSplit(50);
        conf1.setIvaratorMaxOpenFiles(200);
        conf1.setIvaratorNumRetries(3);
        conf1.setIvaratorPersistVerify(true);
        conf1.setIvaratorPersistVerifyCount(10);
        conf1.setMaxIvaratorSources(1000L);
        conf1.setMaxIvaratorSourceWait(15000L);
        conf1.setMaxIvaratorResults(1000000L);

        IvaratorConfig conf2 = new IvaratorConfig();
        IvaratorConfig conf3 = new IvaratorConfig(conf2);

        Assertions.assertNotEquals(conf1, conf2);
        Assertions.assertEquals(conf2, conf3);

    }

    @Test
    public void testWithinShardQueryConfiguration() {
        IvaratorConfig ivaratorConfig = new IvaratorConfig();

        ShardQueryConfiguration queryConfig = new ShardQueryConfiguration();
        queryConfig.setIvaratorConfig(ivaratorConfig);
        queryConfig.setIvaratorMaxOpenFiles(123);

        Assertions.assertEquals(123, ivaratorConfig.getIvaratorMaxOpenFiles());

        ivaratorConfig.setIvaratorMaxOpenFiles(456);
        Assertions.assertEquals(456, queryConfig.getIvaratorMaxOpenFiles());
    }

    @Test
    public void testWithinQueryOptions() {
        IvaratorConfig ivaratorConfig = new IvaratorConfig();

        QueryOptions ops = new QueryOptions();
        ops.setIvaratorConfig(ivaratorConfig);
        ops.setIvaratorMaxOpenFiles(123);

        Assertions.assertEquals(123, ivaratorConfig.getIvaratorMaxOpenFiles());

        ivaratorConfig.setIvaratorMaxOpenFiles(456);
        Assertions.assertEquals(456, ops.getIvaratorMaxOpenFiles());
    }

}
