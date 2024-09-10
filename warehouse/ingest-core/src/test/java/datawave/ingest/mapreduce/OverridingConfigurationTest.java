package datawave.ingest.mapreduce;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import datawave.ingest.mapreduce.job.OverridingConfiguration;

public class OverridingConfigurationTest {

    @Test
    public void shouldOverrideConfs() {
        Configuration base = new Configuration();
        base.set("test.table.name", "new table");
        base.set("flag", "old flag");
        base.set("testflag", "new flag"); // This should not override b/c it's missing the '.'
        base.set("table.name", "old table");

        OverridingConfiguration conf = new OverridingConfiguration("test", base);

        assertThat(conf.get("table.name"), is("new table"));
        assertThat(conf.get("flag"), is("old flag"));
    }
}
