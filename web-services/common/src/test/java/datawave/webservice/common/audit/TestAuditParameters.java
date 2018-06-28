package datawave.webservice.common.audit;

import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.webservice.common.audit.AuditParameters;
import datawave.security.authorization.DatawavePrincipal;
import java.util.Collections;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.ws.rs.core.MultivaluedMap;
import java.util.Arrays;
import java.util.List;

public class TestAuditParameters {
    
    AuditParameters auditParameters = null;
    private MultivaluedMap<String,String> paramsMap = null;
    DatawavePrincipal principal = null;
    private final String userDN = "Last First Middle uid";
    private final String[] auths = new String[] {"AUTHS1", "AUTH2", "AUTH3"};
    
    @Before
    public void setup() {
        this.auditParameters = new AuditParameters();
        this.paramsMap = new MultivaluedMapImpl<>();
        resetParamsMap(this.paramsMap);
        
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of(userDN, "<CN=MY_CA, OU=MY_SUBDIVISION, OU=MY_DIVISION, O=ORG, C=US>"),
                        DatawaveUser.UserType.USER, Arrays.asList(auths), null, null, 0L);
        principal = new DatawavePrincipal(Collections.singletonList(user));
    }
    
    private void resetParamsMap(MultivaluedMap<String,String> paramsMap) {
        paramsMap.clear();
        paramsMap.putSingle(AuditParameters.USER_DN, "Last First Middle uid");
        paramsMap.putSingle(AuditParameters.QUERY_STRING, "FIELD1:VALUE1 AND FIELD2:VALUE2");
        paramsMap.putSingle(AuditParameters.QUERY_AUTHORIZATIONS, "AUTHS1,AUTH2,AUTH3");
        paramsMap.putSingle(AuditParameters.QUERY_AUDIT_TYPE, "PASSIVE");
        paramsMap.putSingle(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, "(AUTH1&AUTH2)");
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
            this.paramsMap.add(p, this.paramsMap.getFirst(p));
            try {
                this.auditParameters.validate(this.paramsMap);
            } catch (Exception e) {
                Assert.assertEquals(IllegalArgumentException.class.getName(), e.getClass().getName());
                Assert.assertEquals("Required parameter " + p + " only accepts one value", e.getMessage());
            }
        }
    }
    
    @Test
    public void handlesSpacesInAuths() {
        this.paramsMap.remove(AuditParameters.QUERY_AUTHORIZATIONS);
        this.paramsMap.putSingle(AuditParameters.QUERY_AUTHORIZATIONS, "AUTH1, AUTH2, AUTH3");
        this.auditParameters.validate(this.paramsMap);
        Assert.assertEquals("AUTH1,AUTH2,AUTH3", this.auditParameters.getAuths());
    }
    
    @Test
    public void handlesBlanksInAuths() {
        this.paramsMap.remove(AuditParameters.QUERY_AUTHORIZATIONS);
        this.paramsMap.putSingle(AuditParameters.QUERY_AUTHORIZATIONS, "AUTH1,,AUTH2,,AUTH3");
        this.auditParameters.validate(this.paramsMap);
        Assert.assertEquals("AUTH1,AUTH2,AUTH3", this.auditParameters.getAuths());
    }
    
    @Test
    public void validateDowngradedAuths() {
        this.paramsMap.remove(AuditParameters.QUERY_AUTHORIZATIONS);
        this.paramsMap.putSingle(AuditParameters.QUERY_AUTHORIZATIONS, "AUTHS1,AUTH2");
        this.auditParameters.setPrincipal(principal);
        this.auditParameters.validate(this.paramsMap);
        Assert.assertEquals("AUTHS1,AUTH2", this.auditParameters.getAuths());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void validateMissingAuths() {
        this.paramsMap.remove(AuditParameters.QUERY_AUTHORIZATIONS);
        this.paramsMap.putSingle(AuditParameters.QUERY_AUTHORIZATIONS, "AUTH7,AUTH8,AUTH9");
        this.auditParameters.setPrincipal(principal);
        this.auditParameters.validate(this.paramsMap);
    }
    
    @Test
    public void validateNoAuths() {
        this.paramsMap.remove(AuditParameters.QUERY_AUTHORIZATIONS);
        this.paramsMap.putSingle(AuditParameters.QUERY_AUTHORIZATIONS, "");
        this.auditParameters.setPrincipal(principal);
        this.auditParameters.validate(this.paramsMap);
        Assert.assertEquals("AUTHS1,AUTH2,AUTH3", this.auditParameters.getAuths());
    }
}
