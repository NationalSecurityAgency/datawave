package datawave.microservice.audit.common;

import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import datawave.webservice.common.audit.Auditor.AuditType;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.Test;

import java.util.Date;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class AuditMessageHandlerTest {
    
    @Test
    public void onMessageTest() throws Exception {
        AuditParameters auditParams = new AuditParameters();
        auditParams.setUserDn("someUser");
        auditParams.setAuths("AUTH1,AUTH2");
        auditParams.setQuery("test query");
        auditParams.setAuditType(AuditType.ACTIVE);
        auditParams.setColviz(new ColumnVisibility("ALL"));
        auditParams.setQueryDate(new Date());
        
        TestAuditor auditor = new TestAuditor();
        
        AuditMessageHandler auditMessageHandler = new AuditMessageHandler(new AuditParameters(), auditor);
        
        auditMessageHandler.onMessage(AuditMessage.fromParams(auditParams));
        
        Map<String,String> received = auditor.getAuditParameters().toMap();
        Map<String,String> expected = auditParams.toMap();
        
        for (String param : expected.keySet()) {
            assertEquals(expected.get(param), received.get(param));
            received.remove(param);
        }
        
        assertEquals(0, received.size());
    }
    
    private static class TestAuditor implements Auditor {
        
        AuditParameters auditParameters;
        
        @Override
        public void audit(AuditParameters msg) throws Exception {
            this.auditParameters = msg;
        }
        
        public AuditParameters getAuditParameters() {
            return auditParameters;
        }
    }
}
