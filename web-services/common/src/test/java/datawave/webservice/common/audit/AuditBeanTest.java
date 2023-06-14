package datawave.webservice.common.audit;

import org.jboss.resteasy.specimpl.MultivaluedMapImpl;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import javax.ws.rs.core.MultivaluedMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class AuditBeanTest {

    private AuditBean auditBean;
    private TestAuditor auditor;

    private Map<String,List<String>> params = new HashMap<>();

    @Before
    public void setup() {
        auditBean = new AuditBean();
        auditor = new TestAuditor();

        Whitebox.setInternalState(auditBean, AuditParameterBuilder.class, new DefaultAuditParameterBuilder());
        Whitebox.setInternalState(auditBean, AuditService.class, auditor);

        params.put(AuditParameters.USER_DN, Collections.singletonList("someUser"));
        params.put(AuditParameters.QUERY_STRING, Collections.singletonList("someQuery"));
        params.put(AuditParameters.QUERY_SELECTORS, Arrays.asList("sel1", "sel2"));
        params.put(AuditParameters.QUERY_AUTHORIZATIONS, Collections.singletonList("AUTH1,AUTH2"));
        params.put(AuditParameters.QUERY_AUDIT_TYPE, Collections.singletonList(Auditor.AuditType.ACTIVE.name()));
        params.put(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, Collections.singletonList("ALL"));
        params.put(AuditParameters.QUERY_DATE, Collections.singletonList(Long.toString(new Date().getTime())));

    }

    @Test
    public void restCallTest() {
        MultivaluedMap<String,String> mvMapParams = new MultivaluedMapImpl<>();
        mvMapParams.putAll(params);
        auditBean.auditRest(mvMapParams);

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
    public void internalCallTest() {
        MultivaluedMap<String,String> paramsMap = new MultivaluedMapImpl<>();
        paramsMap.putAll(params);
        AuditParameters auditParams = new AuditParameters();
        auditParams.validate(params);
        auditBean.auditRest(paramsMap);

        AuditParameters actual = new AuditParameters();
        actual.validate(AuditParameters.parseMessage(auditor.params));

        assertEquals(auditParams.getUserDn(), actual.getUserDn());
        assertEquals(auditParams.getQuery(), actual.getQuery());
        assertEquals(auditParams.getSelectors(), actual.getSelectors());
        assertEquals(auditParams.getAuths(), actual.getAuths());
        assertEquals(auditParams.getAuditType(), actual.getAuditType());
        assertEquals(auditParams.getColviz(), actual.getColviz());
    }

    private static class TestAuditor implements AuditService {

        Map<String,String> params;

        @Override
        public String audit(Map<String,String> parameters) {
            this.params = parameters;
            return UUID.randomUUID().toString();
        }
    }
}
