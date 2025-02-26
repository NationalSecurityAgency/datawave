package datawave.user;

import java.util.TreeSet;

import org.junit.Before;
import org.junit.Test;

import datawave.webservice.ProtobufSerializationTestBase;

public class UserAuthorizationsTest extends ProtobufSerializationTestBase {
    @Before
    public void setup() {
        super.setUp();
    }
    
    @Test
    public void testFieldConfiguration() {
        String[] expecteds = new String[] {"SCHEMA", "auths", "serialVersionUID"};
        testFieldNames(expecteds, UserAuthorizations.class);
    }
    
    @Test
    public void testSerialization() throws Exception {
        TreeSet<String> auths = new TreeSet<String>();
        auths.add("a1");
        auths.add("a2");
        auths.add("a3");
        testRoundTrip(UserAuthorizations.class, new String[] {"auths"}, new Object[] {auths});
    }
}
