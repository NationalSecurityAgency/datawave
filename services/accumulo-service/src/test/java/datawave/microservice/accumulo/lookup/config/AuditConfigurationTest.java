package datawave.microservice.accumulo.lookup.config;

import datawave.webservice.common.audit.Auditor;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AuditConfigurationTest {
    
    @Test
    public void testActiveAuditOfIndexTable() {
        
        LookupAuditProperties.AuditConfiguration l1 = new LookupAuditProperties.AuditConfiguration("myIi_.*", null, null, null, Auditor.AuditType.ACTIVE);
        assertTrue(l1.isMatch("myIi_201201", null, null, null));
    }
    
    @Test
    public void testNoAuditOfColFamInTable() {
        
        LookupAuditProperties.AuditConfiguration l2 = new LookupAuditProperties.AuditConfiguration("shard", "[0-9]{8}_[0-9]+", "d", ".*CONTENT",
                        Auditor.AuditType.NONE);
        assertTrue(l2.isMatch("shard", "20120112_100", "d", "datatype\\x00-103s6q.-y9weab.-50ho6n\\x00CONTENT"));
        assertFalse(l2.isMatch("shard", "20120112_100", null, null));
    }
}
