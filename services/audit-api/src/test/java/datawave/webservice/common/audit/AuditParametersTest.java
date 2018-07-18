package datawave.webservice.common.audit;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class AuditParametersTest {
    
    private Map<String,List<String>> params = new HashMap<>();
    
    public AuditParametersTest() {
        params.put(AuditParameters.USER_DN, Arrays.asList("someUser"));
        params.put(AuditParameters.QUERY_STRING, Arrays.asList("someQuery"));
        params.put(AuditParameters.QUERY_SELECTORS, Arrays.asList("sel1", "sel2"));
        params.put(AuditParameters.QUERY_AUTHORIZATIONS, Arrays.asList("AUTH1,AUTH2"));
        params.put(AuditParameters.QUERY_AUDIT_TYPE, Arrays.asList(Auditor.AuditType.ACTIVE.name()));
        params.put(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, Arrays.asList("ALL"));
        params.put(AuditParameters.QUERY_DATE, Arrays.asList(Long.toString(new Date().getTime())));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void missingUserDNTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.remove(AuditParameters.USER_DN);
        
        new AuditParameters().validate(params);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void missingQueryStringTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.remove(AuditParameters.QUERY_STRING);
        
        new AuditParameters().validate(params);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void missingAuthsTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.remove(AuditParameters.QUERY_AUTHORIZATIONS);
        
        new AuditParameters().validate(params);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void missingAuditTypeTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.remove(AuditParameters.QUERY_AUDIT_TYPE);
        
        new AuditParameters().validate(params);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void missingColVizTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.remove(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ);
        
        new AuditParameters().validate(params);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void nullUserDNTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.USER_DN, null);
        
        new AuditParameters().validate(params);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void nullQueryStringTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_STRING, null);
        
        new AuditParameters().validate(params);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void nullAuthsTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_AUTHORIZATIONS, null);
        
        new AuditParameters().validate(params);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void nullAuditTypeTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_AUDIT_TYPE, null);
        
        new AuditParameters().validate(params);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void nullColVizTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, null);
        
        new AuditParameters().validate(params);
    }
    
    @Test(expected = NullPointerException.class)
    public void nullValueUserDNTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.USER_DN, Arrays.asList(null));
        
        new AuditParameters().validate(params);
    }
    
    @Test(expected = NullPointerException.class)
    public void nullValueQueryStringTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_STRING, Arrays.asList(null));
        
        new AuditParameters().validate(params);
    }
    
    @Test(expected = NullPointerException.class)
    public void nullValueAuthsTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_AUTHORIZATIONS, Arrays.asList(null));
        
        new AuditParameters().validate(params);
    }
    
    @Test(expected = NullPointerException.class)
    public void nullValueAuditTypeTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_AUDIT_TYPE, Arrays.asList(null));
        
        new AuditParameters().validate(params);
    }
    
    @Test(expected = NullPointerException.class)
    public void nullValueColVizTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, Arrays.asList(null));
        
        new AuditParameters().validate(params);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void emptyUserDNTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.USER_DN, new ArrayList<String>());
        
        new AuditParameters().validate(params);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void emptyQueryStringTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_STRING, new ArrayList<String>());
        
        new AuditParameters().validate(params);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void emptyAuthsTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_AUTHORIZATIONS, new ArrayList<String>());
        
        new AuditParameters().validate(params);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void emptyAuditTypeTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_AUDIT_TYPE, new ArrayList<String>());
        
        new AuditParameters().validate(params);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void emptyColVizTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, new ArrayList<String>());
        
        new AuditParameters().validate(params);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void multiValueUserDNTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.USER_DN, Arrays.asList("userDN1", "userDN2"));
        
        new AuditParameters().validate(params);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void multiValueQueryStringTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_STRING, Arrays.asList("query1", "query2"));
        
        new AuditParameters().validate(params);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void multiValueAuthsTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_AUTHORIZATIONS, Arrays.asList("AUTH1", "AUTH2"));
        
        new AuditParameters().validate(params);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void multiValueAuditTypeTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_AUDIT_TYPE, Arrays.asList(Auditor.AuditType.ACTIVE.name(), Auditor.AuditType.PASSIVE.name()));
        
        new AuditParameters().validate(params);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void multiValueColVizTest() {
        Map<String,List<String>> params = new HashMap<>(this.params);
        params.put(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, Arrays.asList("ALL", "NONE"));
        
        new AuditParameters().validate(params);
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
