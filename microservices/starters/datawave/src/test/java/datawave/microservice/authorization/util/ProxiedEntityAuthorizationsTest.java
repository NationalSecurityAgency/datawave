package datawave.microservice.authorization.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.junit.jupiter.api.Test;

import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.SubjectIssuerDNPair;

/**
 * Exercises the authorizations passed to a query for the same records and entity auths used by the Docker Compose checks.
 */
public class ProxiedEntityAuthorizationsTest {
    private static final DatawaveUser USER_ONE = user("proxy user one", UserType.USER, "A", "B", "C", "D");
    private static final DatawaveUser USER_TWO = user("proxy user two", UserType.USER, "A", "B", "E", "F");
    private static final DatawaveUser SERVER_ONE = user("proxy server one", UserType.SERVER, "A", "B", "C", "E", "G");
    private static final DatawaveUser SERVER_TWO = user("proxy server two", UserType.SERVER, "A", "B", "H");

    private static final Map<String,ColumnVisibility> RECORDS = records();

    @Test
    public void oneUserThroughOneNpe() throws Exception {
        DatawaveUserDetails chain = chain(USER_ONE, SERVER_ONE);

        assertVisible(chain, "A,B,C", "COMMON", "USER_ONE_SERVER_ONE");
        assertVisible(chain, "A,B,C,D", "COMMON", "USER_ONE_SERVER_ONE");
        assertThrows(AuthorizationException.class, () -> AuthorizationsUtil.getDowngradedAuthorizations("A,B,C,G", chain, chain));
    }

    @Test
    public void multipleUsersThroughOneNpe() throws Exception {
        DatawaveUserDetails firstChain = chain(USER_ONE, SERVER_ONE);
        DatawaveUserDetails secondChain = chain(USER_TWO, SERVER_ONE);

        assertVisible(firstChain, "A,B,C", "COMMON", "USER_ONE_SERVER_ONE");
        assertVisible(secondChain, "A,B,E", "COMMON", "USER_TWO_SERVER_ONE");
        assertVisible(secondChain, "A,B,E,F", "COMMON", "USER_TWO_SERVER_ONE");
        assertThrows(AuthorizationException.class, () -> AuthorizationsUtil.getDowngradedAuthorizations("A,B,E,G", secondChain, secondChain));
    }

    @Test
    public void oneUserThroughMultipleNpes() throws Exception {
        DatawaveUserDetails chain = chain(USER_ONE, SERVER_ONE, SERVER_TWO);

        assertVisible(chain, "A,B", "COMMON");
        assertVisible(chain, "A,B,C", "COMMON");
        assertThrows(AuthorizationException.class, () -> AuthorizationsUtil.getDowngradedAuthorizations("A,B,C,G", chain, chain));
    }

    private static DatawaveUser user(String name, UserType type, String... auths) {
        SubjectIssuerDNPair dn = SubjectIssuerDNPair.of("cn=" + name, "cn=example ca");
        return new DatawaveUser(dn, type, Set.of(auths), null, null, System.currentTimeMillis());
    }

    private static DatawaveUserDetails chain(DatawaveUser... users) {
        return new DatawaveUserDetails(List.of(users));
    }

    private static void assertVisible(DatawaveUserDetails chain, String requested, String... expected) throws AuthorizationException, VisibilityParseException {
        Collection<Authorizations> queryAuths = AuthorizationsUtil.getDowngradedAuthorizations(requested, chain, chain);
        Set<String> actual = RECORDS.entrySet().stream().filter(entry -> visibleToAll(entry.getValue(), queryAuths)).map(Map.Entry::getKey)
                        .collect(Collectors.toSet());
        assertEquals(Set.copyOf(Arrays.asList(expected)), actual);
    }

    private static boolean visibleToAll(ColumnVisibility visibility, Collection<Authorizations> queryAuths) {
        for (Authorizations auths : queryAuths) {
            try {
                if (!new VisibilityEvaluator(auths).evaluate(visibility)) {
                    return false;
                }
            } catch (VisibilityParseException e) {
                throw new IllegalArgumentException("Invalid test visibility", e);
            }
        }
        return true;
    }

    private static Map<String,ColumnVisibility> records() {
        Map<String,ColumnVisibility> records = new LinkedHashMap<>();
        records.put("COMMON", new ColumnVisibility("A&B"));
        records.put("USER_ONE_SERVER_ONE", new ColumnVisibility("A&B&C"));
        records.put("USER_TWO_SERVER_ONE", new ColumnVisibility("A&B&E"));
        records.put("USER_ONE_ONLY", new ColumnVisibility("D"));
        records.put("USER_TWO_ONLY", new ColumnVisibility("F"));
        records.put("SERVER_ONE_ONLY", new ColumnVisibility("G"));
        records.put("SERVER_TWO_ONLY", new ColumnVisibility("H"));
        records.put("USER_ONE_PARTIAL", new ColumnVisibility("A&D"));
        records.put("SERVER_ONE_PARTIAL", new ColumnVisibility("A&G"));
        records.put("SPLIT_USERS_PARTIAL", new ColumnVisibility("C&E"));
        return records;
    }
}
