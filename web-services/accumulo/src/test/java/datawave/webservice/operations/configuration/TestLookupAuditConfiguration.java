package datawave.webservice.operations.configuration;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import datawave.webservice.common.audit.Auditor.AuditType;
import org.junit.Test;

public class TestLookupAuditConfiguration {
    
    @Test
    public void testActiveAuditOfIndexTable() {
        
        LookupAuditConfiguration l1 = new LookupAuditConfiguration("myIi_.*", null, null, null, AuditType.ACTIVE);
        assertTrue(l1.isMatch("myIi_201201", null, null, null));
    }
    
    @Test
    public void testNoAuditOfColFamInTable() {
        
        LookupAuditConfiguration l2 = new LookupAuditConfiguration("shard", "[0-9]{8}_[0-9]+", "d", ".*CONTENT", AuditType.NONE);
        assertTrue(l2.isMatch("shard", "20120112_100", "d", "datatype\\x00-103s6q.-y9weab.-50ho6n\\x00CONTENT"));
        assertFalse(l2.isMatch("shard", "20120112_100", null, null));
    }
}
