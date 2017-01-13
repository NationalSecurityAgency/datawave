package nsa.datawave.webservice.util;

import org.apache.accumulo.core.security.Authorizations;
import org.junit.Assert;
import org.junit.Test;

public class MergeAuthorizationsHelperTest {
    @Test
    public void testAuthMerge() {
        
        Authorizations auths1 = new Authorizations("auth1", "auth2", "auth3");
        Authorizations auths2 = new Authorizations("auth2", "auth3", "auth4");
        
        // expected auths is actually the intersection
        Authorizations expectedAuths = new Authorizations("auth2", "auth3");
        
        Authorizations mergedAuths = MergeAuthorizationsHelper.mergeAuthorizations(auths1, auths2);
        Assert.assertEquals(expectedAuths, mergedAuths);
    }
}
