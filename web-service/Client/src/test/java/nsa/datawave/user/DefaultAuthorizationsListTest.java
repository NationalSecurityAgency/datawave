package nsa.datawave.user;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import nsa.datawave.user.AuthorizationsListBase.SubjectIssuerDNPair;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DefaultAuthorizationsListTest {
    
    private DefaultAuthorizationsList dal;
    
    // Using example dns from https://en.wikipedia.org/wiki/Lightweight_Directory_Access_Protocol
    @SuppressWarnings("unused")
    private final String stock_dn = "cn=Common Name, l=Locality, ou=Some Organizational Unit, o=Some Organization, st=CA, c=US";
    
    private TreeSet<String> treeForTests;
    private LinkedHashMap<SubjectIssuerDNPair,Set<String>> authMapForTests;
    private static final String TITLE = "Effective Authorizations", EMPTY = "";
    
    @Before
    public void beforeTests() {
        treeForTests = new TreeSet<String>();
        treeForTests.clear(); // Default to empty treeSet
        
        authMapForTests = new LinkedHashMap<SubjectIssuerDNPair,Set<String>>();
        authMapForTests.clear(); // Default to empty LinkedHashMap
    }
    
    @SuppressWarnings("static-access")
    @Test
    public void testEmptyConstructor() {
        dal = new DefaultAuthorizationsList();
        
        Assert.assertEquals(treeForTests, dal.getAllAuths());
        Assert.assertEquals(authMapForTests, dal.getAuths());
        Assert.assertEquals(EMPTY, dal.getHeadContent());
        Assert.assertEquals(TITLE, dal.getTitle());
        Assert.assertEquals(TITLE, dal.getPageHeader());
        
        Set<String> auths = new HashSet<String>();
        auths.add("dnAuths1");
        auths.add("dnAuths2");
        dal.addAuths("DN", "issuerDN", auths);
        dal.addAuths("DN2", "issuerDN2", auths);
        dal.setUserAuths("DN", "issuerDN", auths);
        
        Assert.assertEquals(2, dal.getAllAuths().size());
        Assert.assertEquals(2, dal.getAuths().size());
        Assert.assertEquals(EMPTY, dal.getHeadContent());
        Assert.assertEquals(TITLE, dal.getTitle());
        Assert.assertEquals(TITLE, dal.getPageHeader());
        
        Map<String,Set<String>> authMapping = new HashMap<String,Set<String>>();
        authMapping.put("authMapKey", auths);
        authMapping.put("authMapKey2", auths);
        dal.setAuthMapping(authMapping);
        
        Assert.assertEquals(2, dal.getAuthMapping().size());
        
        String toStringExpected = "userAuths=[dnAuths1, dnAuths2], entityAuths=[DN<issuerDN>=[dnAuths1, dnAuths2]DN2<issuerDN2>=[dnAuths1, dnAuths2]], authMapping=[authMapKey->(dnAuths1,dnAuths2,), authMapKey2->(dnAuths1,dnAuths2,), ]";
        
        Assert.assertEquals(toStringExpected, dal.toString());
        
        String mcExpected = "<h2>Auths for user Subject: DN (Issuer issuerDN)</h2><table><tr><td>dnAuths1</td><td>dnAuths2</td></tr></table><h2>Auths for Subject: DN2 (Issuer: issuerDN2)</h2><table><tr><td>dnAuths1</td><td>dnAuths2</td></tr></table><h2>Roles to Accumulo Auths</h2><table><tr><th>Role</th><th>Accumulo Authorizations</th></tr><tr><td>authMapKey</td><td>dnAuths1,dnAuths2</td></tr><tr class=\"highlight\"><td>authMapKey2</td><td>dnAuths1,dnAuths2</td></tr></table>";
        String mainContent = dal.getMainContent();
        Assert.assertEquals(mcExpected, mainContent);
        
        // Tests for the SCHEMA nested class
        Assert.assertEquals(null, dal.getSchema().getFieldName(0));
        Assert.assertEquals("auths", dal.getSchema().getFieldName(1));
        Assert.assertEquals("authMapping", dal.getSchema().getFieldName(2));
        
        Assert.assertEquals(0, dal.getSchema().getFieldNumber("garbage"));
        Assert.assertEquals(1, dal.getSchema().getFieldNumber("auths"));
        Assert.assertEquals(2, dal.getSchema().getFieldNumber("authMapping"));
        
        Assert.assertEquals(DefaultAuthorizationsList.class, dal.getSchema().typeClass());
        Assert.assertEquals(DefaultAuthorizationsList.class.getSimpleName(), dal.getSchema().messageName());
        Assert.assertEquals(DefaultAuthorizationsList.class.getName(), dal.getSchema().messageFullName());
        Assert.assertEquals(true, dal.getSchema().isInitialized(null));
        Assert.assertEquals(true, dal.cachedSchema().isInitialized(null));
        
        // Testing the bugfix for DefaultAuthorizationsList with mock data
        String random = "ABC|DEF|GHI|JKL|MNO";
        
        String[] splits = random.split("\\|");
        Assert.assertEquals(5, splits.length);
        
        splits = random.split("|");
        Assert.assertNotEquals(5, splits.length);
    }
}
