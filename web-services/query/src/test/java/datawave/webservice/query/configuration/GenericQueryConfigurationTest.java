package datawave.webservice.query.configuration;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Date;

import org.apache.accumulo.core.security.Authorizations;
import org.junit.Test;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.util.TableName;

public class GenericQueryConfigurationTest {

    @Test
    public void testCopyConstructor() {
        GenericQueryConfiguration config = new GenericQueryConfiguration();
        config.setAuthorizations(Collections.singleton(new Authorizations("AUTH1,AUTH2")));
        config.setQueryString("FOO == 'bar'");
        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));
        config.setMaxWork(Long.MAX_VALUE);
        config.setBaseIteratorPriority(17);
        config.setTableName(TableName.SHARD_INDEX); // non-default value
        // skip query data iterator, empty iterator doesn't matter
        config.setBypassAccumulo(true);
        config.setAccumuloPassword("env:PASS");

        GenericQueryConfiguration copy = new GenericQueryConfiguration(config);
        assertEquals(config.getAuthorizations(), copy.getAuthorizations());
        assertEquals(config.getQueryString(), copy.getQueryString());
        assertEquals(config.getBeginDate(), copy.getBeginDate());
        assertEquals(config.getEndDate(), copy.getEndDate());
        assertEquals(config.getMaxWork(), copy.getMaxWork());
        assertEquals(config.getBaseIteratorPriority(), copy.getBaseIteratorPriority());
        assertEquals(config.getTableName(), copy.getTableName());
        assertEquals(config.getBypassAccumulo(), copy.getBypassAccumulo());
        assertEquals(config.getAccumuloPassword(), copy.getAccumuloPassword());
    }

}
