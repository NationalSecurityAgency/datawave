package datawave.user;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

import datawave.user.AuthorizationsListBase.SubjectIssuerDNPair;

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
        
        Assert.assertTrue(dal.getAllAuths().isEmpty());
        Assert.assertTrue(dal.getAuths().isEmpty());
        
        Assert.assertEquals(EMPTY, dal.getHeadContent());
        Assert.assertEquals(TITLE, dal.getTitle());
        Assert.assertEquals(TITLE, dal.getPageHeader());
        
        Set<String> auths = Sets.newHashSet("dnAuths1", "dnAuths2");
        dal.addAuths("DN", "issuerDN", auths);
        dal.addAuths("DN2", "issuerDN2", auths);
        dal.setUserAuths("DN", "issuerDN", auths);
        
        Assert.assertEquals(2, dal.getAllAuths().size());
        Assert.assertTrue(CollectionUtils.isEqualCollection(dal.getAllAuths(), auths));
        Assert.assertEquals(2, dal.getAuths().size());
        Assert.assertEquals(EMPTY, dal.getHeadContent());
        Assert.assertEquals(TITLE, dal.getTitle());
        Assert.assertEquals(TITLE, dal.getPageHeader());
        
        // Test getAuths()
        LinkedHashMap<SubjectIssuerDNPair,Set<String>> expectedGetAuths = new LinkedHashMap<SubjectIssuerDNPair,Set<String>>();
        expectedGetAuths.put(new SubjectIssuerDNPair("DN", "issuerDN"), new TreeSet<String>(auths));
        expectedGetAuths.put(new SubjectIssuerDNPair("DN2", "issuerDN2"), new TreeSet<String>(auths));
        Assert.assertTrue(CollectionUtils.isEqualCollection(dal.getAuths().entrySet(), expectedGetAuths.entrySet()));
        
        // Test userAuths
        Assert.assertTrue(CollectionUtils.isEqualCollection(dal.userAuths, Sets.newHashSet("dnAuths1", "dnAuths2")));
        
        Map<String,Collection<String>> authMapping = new HashMap<String,Collection<String>>();
        authMapping.put("authMapKey", auths);
        authMapping.put("authMapKey2", auths);
        dal.setAuthMapping(authMapping);
        
        Assert.assertEquals(2, dal.getAuthMapping().size());
        Assert.assertTrue(CollectionUtils.isEqualCollection(dal.getAuthMapping().entrySet(), authMapping.entrySet()));
        
        // toString() matching is not a good test when the underlying data structure does not use ordered collections.
        // tested individual pieces above should suffice
        // String toStringExpected =
        // "userAuths=[dnAuths1, dnAuths2], entityAuths=[DN<issuerDN>=[dnAuths1, dnAuths2]DN2<issuerDN2>=[dnAuths1, dnAuths2]],
        // authMapping=[authMapKey->(dnAuths1,dnAuths2,), authMapKey2->(dnAuths1,dnAuths2,), ]";
        // Assert.assertEquals(toStringExpected, dal.toString());
        
        String mcExpected = "<h2>Auths for user Subject: DN (Issuer issuerDN)</h2><table><tr><td>dnAuths1</td><td>dnAuths2</td></tr></table><h2>Auths for Subject: DN2 (Issuer: issuerDN2)</h2><table><tr><td>dnAuths1</td><td>dnAuths2</td></tr></table><h2>Roles to Accumulo Auths</h2><table><tr><th>Role</th><th>Accumulo Authorizations</th></tr><tr><td>authMapKey</td><td>dnAuths1,dnAuths2</td></tr><tr class=\"highlight\"><td>authMapKey2</td><td>dnAuths1,dnAuths2</td></tr></table>";
        String mainContent = dal.getMainContent();
        // Testing expected string with an unordered collection doesn't always work, we'll need to parse this as we go
        // Assert.assertEquals(mcExpected, mainContent);
        Assert.assertEquals("<h2>Auths for user Subject: DN (Issuer issuerDN)</h2><table><tr>", mainContent.substring(0, 64));
        Assert.assertTrue("<td>dnAuths1</td><td>dnAuths2</td>".equals(mainContent.substring(64, 98))
                        || "<td>dnAuths2</td><td>dnAuths1</td>".equals(mainContent.substring(64, 98)));
        Assert.assertEquals("</tr></table><h2>Auths for Subject: DN2 (Issuer: issuerDN2)</h2><table><tr>", mainContent.substring(98, 173));
        Assert.assertTrue("<td>dnAuths1</td><td>dnAuths2</td>".equals(mainContent.substring(173, 207))
                        || "<td>dnAuths2</td><td>dnAuths1</td>".equals(mainContent.substring(173, 207)));
        Assert.assertEquals("</tr></table><h2>Roles to Accumulo Auths</h2><table><tr><th>Role</th><th>Accumulo Authorizations</th></tr><tr>",
                        mainContent.substring(207, 317));
        Assert.assertTrue("<td>authMapKey</td><td>dnAuths1,dnAuths2</td>".equals(mainContent.substring(317, 362))
                        || "<td>authMapKey</td><td>dnAuths2,dnAuths1</td>".equals(mainContent.substring(317, 362))
                        || "<td>authMapKey2</td><td>dnAuths1,dnAuths2</td>".equals(mainContent.substring(317, 362))
                        || "<td>authMapKey2</td><td>dnAuths2,dnAuths1</td>".equals(mainContent.substring(317, 362)));
        Assert.assertEquals("</tr><tr class=\"highlight\">", mainContent.substring(362, 389));
        Assert.assertTrue("<td>authMapKey</td><td>dnAuths1,dnAuths2</td>".equals(mainContent.substring(389, 435))
                        || "<td>authMapKey</td><td>dnAuths2,dnAuths1</td>".equals(mainContent.substring(389, 435))
                        || "<td>authMapKey2</td><td>dnAuths1,dnAuths2</td>".equals(mainContent.substring(389, 435))
                        || "<td>authMapKey2</td><td>dnAuths2,dnAuths1</td>".equals(mainContent.substring(389, 435)));
        Assert.assertEquals("</tr></table>", mainContent.substring(435));
        
        // Tests for the SCHEMA nested class
        Assert.assertNull(dal.getSchema().getFieldName(0));
        Assert.assertEquals("auths", dal.getSchema().getFieldName(1));
        Assert.assertEquals("authMapping", dal.getSchema().getFieldName(2));
        
        Assert.assertEquals(0, dal.getSchema().getFieldNumber("garbage"));
        Assert.assertEquals(1, dal.getSchema().getFieldNumber("auths"));
        Assert.assertEquals(2, dal.getSchema().getFieldNumber("authMapping"));
        
        Assert.assertEquals(DefaultAuthorizationsList.class, dal.getSchema().typeClass());
        Assert.assertEquals(DefaultAuthorizationsList.class.getSimpleName(), dal.getSchema().messageName());
        Assert.assertEquals(DefaultAuthorizationsList.class.getName(), dal.getSchema().messageFullName());
        Assert.assertTrue(dal.getSchema().isInitialized(null));
        Assert.assertTrue(dal.cachedSchema().isInitialized(null));
        
    }
    
    @Test
    public void testDefaultAuthList() {
        // Testing the bugfix for DefaultAuthorizationsList with mock data
        String random = "ABC|DEF|GHI|JKL|MNO";
        
        String[] splits = random.split("\\|");
        Assert.assertEquals(5, splits.length);
        
        splits = random.split("|");
        Assert.assertNotEquals(5, splits.length);
    }
}
