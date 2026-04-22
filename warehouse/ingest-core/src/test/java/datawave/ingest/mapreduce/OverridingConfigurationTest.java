package datawave.ingest.mapreduce;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import datawave.ingest.mapreduce.job.OverridingConfiguration;

public class OverridingConfigurationTest {

    @Test
    public void shouldOverrideConfigs() {
        Configuration base = new Configuration();
        base.set("test.table.name", "new table");
        base.set("flag", "old flag");
        base.set("testflag", "new flag"); // This should not override b/c it's missing the '.'
        base.set("table.name", "old table");

        OverridingConfiguration conf = new OverridingConfiguration("test", base);

        assertEquals("new table", conf.get("table.name"));
        assertEquals("old flag", conf.get("flag"));
    }
}
