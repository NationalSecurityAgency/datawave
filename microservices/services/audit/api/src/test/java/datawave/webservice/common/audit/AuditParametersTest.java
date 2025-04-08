package datawave.webservice.common.audit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.jupiter.api.Test;

public class AuditParametersTest {
    
    private Map<String,List<String>> params = new HashMap<>();
    
    public AuditParametersTest() {
        params.put(AuditParameters.USER_DN, Collections.singletonList("someUser"));
        params.put(AuditParameters.QUERY_STRING, Collections.singletonList("someQuery"));
        params.put(AuditParameters.QUERY_SELECTORS, Arrays.asList("sel1", "sel2"));
        params.put(AuditParameters.QUERY_AUTHORIZATIONS, Collections.singletonList("AUTH1,AUTH2"));
        params.put(AuditParameters.QUERY_AUDIT_TYPE, Collections.singletonList(Auditor.AuditType.ACTIVE.name()));
        params.put(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, Collections.singletonList("ALL"));
        params.put(AuditParameters.QUERY_DATE, Collections.singletonList(Long.toString(new Date().getTime())));
    }
    
    @Test
    public void testValidateSetsNonRequiredParameters() {
        params.put(AuditParameters.AUDIT_ID, Collections.singletonList(UUID.randomUUID().toString()));
        params.put(AuditParameters.QUERY_LOGIC_CLASS, Collections.singletonList("QueryLogicClass"));
        
        AuditParameters p = new AuditParameters();
        p.validate(params);
        
        assertEquals(params.get(AuditParameters.AUDIT_ID).get(0), p.getAuditId());
        assertEquals(params.get(AuditParameters.QUERY_DATE).get(0), Long.toString(p.getQueryDate().getTime()));
        assertEquals(params.get(AuditParameters.QUERY_SELECTORS), p.getSelectors());
        assertEquals(params.get(AuditParameters.QUERY_LOGIC_CLASS).get(0), p.getLogicClass());
    }
    
    @Test
    public void missingUserDNTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.remove(AuditParameters.USER_DN);
        
        assertThrows(IllegalArgumentException.class, () -> new AuditParameters().validate(params));
    }
    
    @Test
    public void missingQueryStringTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.remove(AuditParameters.QUERY_STRING);
        
        assertThrows(IllegalArgumentException.class, () -> new AuditParameters().validate(params));
    }
    
    @Test
    public void missingAuthsTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.remove(AuditParameters.QUERY_AUTHORIZATIONS);
        
        assertThrows(IllegalArgumentException.class, () -> new AuditParameters().validate(params));
    }
    
    @Test
    public void missingAuditTypeTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.remove(AuditParameters.QUERY_AUDIT_TYPE);
        
        assertThrows(IllegalArgumentException.class, () -> new AuditParameters().validate(params));
    }
    
    @Test
    public void missingColVizTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.remove(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ);
        
        assertThrows(IllegalArgumentException.class, () -> new AuditParameters().validate(params));
    }
    
    @Test
    public void nullUserDNTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.USER_DN, null);
        
        assertThrows(IllegalArgumentException.class, () -> new AuditParameters().validate(params));
    }
    
    @Test
    public void nullQueryStringTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_STRING, null);
        
        assertThrows(IllegalArgumentException.class, () -> new AuditParameters().validate(params));
    }
    
    @Test
    public void nullAuthsTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_AUTHORIZATIONS, null);
        
        assertThrows(IllegalArgumentException.class, () -> new AuditParameters().validate(params));
    }
    
    @Test
    public void nullAuditTypeTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_AUDIT_TYPE, null);
        
        assertThrows(IllegalArgumentException.class, () -> new AuditParameters().validate(params));
    }
    
    @Test
    public void nullColVizTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, null);
        
        assertThrows(IllegalArgumentException.class, () -> new AuditParameters().validate(params));
    }
    
    @Test
    public void nullValueUserDNTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.USER_DN, Collections.singletonList(null));
        
        assertThrows(NullPointerException.class, () -> new AuditParameters().validate(params));
    }
    
    @Test
    public void nullValueQueryStringTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_STRING, Collections.singletonList(null));
        
        assertThrows(NullPointerException.class, () -> new AuditParameters().validate(params));
    }
    
    @Test
    public void nullValueAuthsTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_AUTHORIZATIONS, Collections.singletonList(null));
        
        assertThrows(NullPointerException.class, () -> new AuditParameters().validate(params));
    }
    
    @Test
    public void nullValueAuditTypeTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_AUDIT_TYPE, Collections.singletonList(null));
        
        assertThrows(NullPointerException.class, () -> new AuditParameters().validate(params));
    }
    
    @Test
    public void nullValueColVizTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, Collections.singletonList(null));
        
        assertThrows(NullPointerException.class, () -> new AuditParameters().validate(params));
    }
    
    @Test
    public void emptyUserDNTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.USER_DN, new ArrayList<>());
        
        assertThrows(IllegalArgumentException.class, () -> new AuditParameters().validate(params));
    }
    
    @Test
    public void emptyQueryStringTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_STRING, new ArrayList<>());
        
        assertThrows(IllegalArgumentException.class, () -> new AuditParameters().validate(params));
    }
    
    @Test
    public void emptyAuthsTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_AUTHORIZATIONS, new ArrayList<>());
        
        assertThrows(IllegalArgumentException.class, () -> new AuditParameters().validate(params));
    }
    
    @Test
    public void emptyAuditTypeTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_AUDIT_TYPE, new ArrayList<>());
        
        assertThrows(IllegalArgumentException.class, () -> new AuditParameters().validate(params));
    }
    
    @Test
    public void emptyColVizTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, new ArrayList<>());
        
        assertThrows(IllegalArgumentException.class, () -> new AuditParameters().validate(params));
    }
    
    @Test
    public void multiValueUserDNTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.USER_DN, Arrays.asList("userDN1", "userDN2"));
        
        assertThrows(IllegalArgumentException.class, () -> new AuditParameters().validate(params));
    }
    
    @Test
    public void multiValueQueryStringTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_STRING, Arrays.asList("query1", "query2"));
        
        assertThrows(IllegalArgumentException.class, () -> new AuditParameters().validate(params));
    }
    
    @Test
    public void multiValueAuthsTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_AUTHORIZATIONS, Arrays.asList("AUTH1", "AUTH2"));
        
        assertThrows(IllegalArgumentException.class, () -> new AuditParameters().validate(params));
    }
    
    @Test
    public void multiValueAuditTypeTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_AUDIT_TYPE, Arrays.asList(Auditor.AuditType.ACTIVE.name(), Auditor.AuditType.PASSIVE.name()));
        
        assertThrows(IllegalArgumentException.class, () -> new AuditParameters().validate(params));
    }
    
    @Test
    public void multiValueColVizTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, Arrays.asList("ALL", "NONE"));
        
        assertThrows(IllegalArgumentException.class, () -> new AuditParameters().validate(params));
    }
    
    @Test
    public void testInvalidDate() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_DATE, Collections.singletonList("invalidDate"));
        
        assertThrows(IllegalArgumentException.class, () -> new AuditParameters().validate(params));
    }
    
    @Test
    public void validateTest() {
        new AuditParameters().validate(params);
    }
    
    @Test
    public void toFromMapTest() {
        AuditParameters auditParams = new AuditParameters();
        auditParams.setUserDn("someUser");
        auditParams.setQuery("someQuery");
        auditParams.setSelectors(Arrays.asList("sel1", "sel2"));
        auditParams.setAuths("AUTH1,AUTH2");
        auditParams.setAuditType(Auditor.AuditType.ACTIVE);
        auditParams.setColviz(new ColumnVisibility("ALL"));
        auditParams.setQueryDate(new Date());
        auditParams.setAuditId(UUID.randomUUID().toString());
        
        AuditParameters fromMapParams = auditParams.fromMap(auditParams.toMap());
        
        assertEquals(auditParams, fromMapParams);
    }
    
    @Test
    public void clearTest() {
        AuditParameters auditParams = new AuditParameters();
        auditParams.setUserDn("someUser");
        auditParams.setQuery("someQuery");
        auditParams.setSelectors(Arrays.asList("sel1", "sel2"));
        auditParams.setAuths("AUTH1,AUTH2");
        auditParams.setAuditType(Auditor.AuditType.ACTIVE);
        auditParams.setColviz(new ColumnVisibility("ALL"));
        auditParams.setQueryDate(new Date());
        auditParams.setAuditId(UUID.randomUUID().toString());
        
        auditParams.clear();
        
        assertNull(auditParams.getUserDn());
        assertNull(auditParams.getQuery());
        assertNull(auditParams.getSelectors());
        assertNull(auditParams.getAuths());
        assertNull(auditParams.getAuditType());
        assertNull(auditParams.getColviz());
        assertNull(auditParams.getQueryDate());
        assertNull(auditParams.getAuditId());
    }
}
