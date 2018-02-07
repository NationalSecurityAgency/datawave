package datawave.webservice.common.audit;

import com.google.common.collect.HashMultimap;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.TestSubject;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.reflect.Whitebox;

import javax.ws.rs.core.MultivaluedMap;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class AuditBeanTest {
    
    private AuditBean auditBean;
    private TestAuditor auditor;
    
    private Map<String,List<String>> params = new HashMap<>();
    
    @Before
    public void setup() {
        auditBean = new AuditBean();
        auditor = new TestAuditor();
        
        Whitebox.setInternalState(auditBean, "auditParameters", new AuditParameters());
        Whitebox.setInternalState(auditBean, "auditor", auditor);
        
        params.put(AuditParameters.USER_DN, Arrays.asList("someUser"));
        params.put(AuditParameters.QUERY_STRING, Arrays.asList("someQuery"));
        params.put(AuditParameters.QUERY_SELECTORS, Arrays.asList("sel1", "sel2"));
        params.put(AuditParameters.QUERY_AUTHORIZATIONS, Arrays.asList("AUTH1,AUTH2"));
        params.put(AuditParameters.QUERY_AUDIT_TYPE, Arrays.asList(Auditor.AuditType.ACTIVE.name()));
        params.put(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, Arrays.asList("ALL"));
        params.put(AuditParameters.QUERY_DATE, Arrays.asList(Long.toString(new Date().getTime())));
        
    }
    
    @Test
    public void restCallTest() {
        MultivaluedMap<String,String> mvMapParams = new MultivaluedMapImpl<>();
        mvMapParams.putAll(params);
        auditBean.audit(mvMapParams);
        
        AuditParameters expected = new AuditParameters();
        expected.validate(mvMapParams);
        
        AuditParameters actual = new AuditParameters();
        actual.validate(AuditParameters.parseMessage(auditor.params));
        
        assertEquals(expected.getUserDn(), actual.getUserDn());
        assertEquals(expected.getQuery(), actual.getQuery());
        assertEquals(expected.getSelectors(), actual.getSelectors());
        assertEquals(expected.getAuths(), actual.getAuths());
        assertEquals(expected.getAuditType(), actual.getAuditType());
        assertEquals(expected.getColviz(), actual.getColviz());
    }
    
    @Test
    public void internalCallTest() throws Exception {
        AuditParameters auditParams = new AuditParameters();
        auditParams.validate(params);
        auditBean.audit(auditParams);
        
        AuditParameters actual = new AuditParameters();
        actual.validate(AuditParameters.parseMessage(auditor.params));
        
        assertEquals(auditParams.getUserDn(), actual.getUserDn());
        assertEquals(auditParams.getQuery(), actual.getQuery());
        assertEquals(auditParams.getSelectors(), actual.getSelectors());
        assertEquals(auditParams.getAuths(), actual.getAuths());
        assertEquals(auditParams.getAuditType(), actual.getAuditType());
        assertEquals(auditParams.getColviz(), actual.getColviz());
    }
    
    private static class TestAuditor implements Auditor {
        
        Map<String,String> params;
        
        @Override
        public void audit(AuditParameters msg) throws Exception {
            params = msg.toMap();
        }
    }
}
