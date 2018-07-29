package datawave.webservice.common.audit;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestAuditParameters {
    
    private AuditParameters auditParameters = null;
    private Map<String,List<String>> paramsMap = null;
    
    @Before
    public void setup() {
        this.auditParameters = new AuditParameters();
        this.paramsMap = new HashMap<>();
        resetParamsMap(this.paramsMap);
    }
    
    private void resetParamsMap(Map<String,List<String>> paramsMap) {
        paramsMap.clear();
        paramsMap.put(AuditParameters.USER_DN, Lists.newArrayList("Last First Middle uid"));
        paramsMap.put(AuditParameters.QUERY_STRING, Lists.newArrayList("FIELD1:VALUE1 AND FIELD2:VALUE2"));
        paramsMap.put(AuditParameters.QUERY_AUTHORIZATIONS, Lists.newArrayList("AUTHS1,AUTH2,AUTH3"));
        paramsMap.put(AuditParameters.QUERY_AUDIT_TYPE, Lists.newArrayList("PASSIVE"));
        paramsMap.put(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, Lists.newArrayList("(AUTH1&AUTH2)"));
    }
    
    @Test
    public void validateAuditParamsSuccess() {
        this.auditParameters.validate(this.paramsMap);
    }
    
    @Test
    public void validateRequiredAuditParamMissing() {
        for (String p : this.auditParameters.getRequiredAuditParameters()) {
            this.resetParamsMap(this.paramsMap);
            this.paramsMap.remove(p);
            try {
                this.auditParameters.validate(this.paramsMap);
            } catch (Exception e) {
                Assert.assertEquals(IllegalArgumentException.class.getName(), e.getClass().getName());
                Assert.assertEquals("Required parameter " + p + " not found", e.getMessage());
            }
        }
    }
    
    @Test
    public void validateRequiredAuditParamDuplicated() {
        for (String p : this.auditParameters.getRequiredAuditParameters()) {
            this.resetParamsMap(this.paramsMap);
            this.paramsMap.get(p).add(this.paramsMap.get(p).get(0));
            try {
                this.auditParameters.validate(this.paramsMap);
            } catch (Exception e) {
                Assert.assertEquals(IllegalArgumentException.class.getName(), e.getClass().getName());
                Assert.assertEquals("Required parameter " + p + " only accepts one value", e.getMessage());
            }
        }
    }
    
    @Test(expected = NullPointerException.class)
    public void validateEmptyAuths() {
        this.paramsMap.remove(AuditParameters.QUERY_AUTHORIZATIONS);
        this.paramsMap.put(AuditParameters.QUERY_AUTHORIZATIONS, Lists.newArrayList(""));
        this.auditParameters.validate(this.paramsMap);
    }
    
    @Test
    public void handlesSpacesInAuths() {
        this.paramsMap.remove(AuditParameters.QUERY_AUTHORIZATIONS);
        this.paramsMap.put(AuditParameters.QUERY_AUTHORIZATIONS, Lists.newArrayList("AUTH1, AUTH2, AUTH3"));
        this.auditParameters.validate(this.paramsMap);
        Assert.assertEquals("AUTH1,AUTH2,AUTH3", this.auditParameters.getAuths());
    }
    
    @Test
    public void handlesBlanksInAuths() {
        this.paramsMap.remove(AuditParameters.QUERY_AUTHORIZATIONS);
        this.paramsMap.put(AuditParameters.QUERY_AUTHORIZATIONS, Lists.newArrayList("AUTH1,,AUTH2,,AUTH3"));
        this.auditParameters.validate(this.paramsMap);
        Assert.assertEquals("AUTH1,AUTH2,AUTH3", this.auditParameters.getAuths());
    }
}
