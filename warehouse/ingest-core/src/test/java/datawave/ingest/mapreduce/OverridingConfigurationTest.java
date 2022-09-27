package datawave.ingest.mapreduce;

import datawave.ingest.mapreduce.job.OverridingConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OverridingConfigurationTest {
    
    @Test
    public void shouldOverrideConfs() {
        Configuration base = new Configuration();
        base.set("test.table.name", "new table");
        base.set("flag", "old flag");
        base.set("testflag", "new flag"); // This should not override b/c it's missing the '.'
        base.set("table.name", "old table");
        
        OverridingConfiguration conf = new OverridingConfiguration("test", base);
        
        assertEquals(conf.get("table.name"), "new table");
        assertEquals(conf.get("flag"), "old flag");
    }
}
