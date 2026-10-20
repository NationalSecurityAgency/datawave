package datawave.query;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.ingest.data.TypeRegistry;
import datawave.microservice.authorization.util.AuthorizationsUtil;
import datawave.query.index.day.IndexIngestUtil;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.ProxiedEntitiesIngest;
import datawave.query.util.ProxiedEntitiesIngest.Event;
import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.SubjectIssuerDNPair;

/**
 * Runs the same user and NPE visibility cases as the Compose checks through ShardQueryLogic.
 */
@ExtendWith(SpringExtension.class)
@ComponentScan(basePackages = "datawave.query")
// @formatter:off
@ContextConfiguration(locations = {
        "classpath:datawave/query/QueryLogicFactory.xml",
        "classpath:MarkingFunctionsContext.xml",
        "classpath:MetadataHelperContext.xml",
        "classpath:CacheContext.xml"})
// @formatter:on
public class ProxiedEntitiesQueryTest extends AbstractQueryTest {
    private static final String QUERY = "CASE == 'proxy'";
    private static final Authorizations ALL_AUTHS = new Authorizations("A", "B", "C", "D", "E", "F", "G", "H");
    private static final DatawaveUser USER_ONE = user("proxy user one", UserType.USER, "A", "B", "C", "D");
    private static final DatawaveUser USER_TWO = user("proxy user two", UserType.USER, "A", "B", "E", "F");
    private static final DatawaveUser SERVER_ONE = user("proxy server one", UserType.SERVER, "A", "B", "C", "E", "G");
    private static final DatawaveUser SERVER_TWO = user("proxy server two", UserType.SERVER, "A", "B", "H");

    private static AccumuloClient client;

    @Autowired
    @Qualifier("EventQuery")
    private ShardQueryLogic logic;

    private DatawavePrincipal principal;
    private String requestedAuths;

    @BeforeAll
    public static void beforeAll() throws Exception {
        client = new InMemoryAccumuloClient("root", new InMemoryInstance(ProxiedEntitiesQueryTest.class.getName()));
        ProxiedEntitiesIngest.write(client);
        new IndexIngestUtil().write(client, ALL_AUTHS);
    }

    @BeforeEach
    public void beforeEach() {
        principal = null;
        requestedAuths = null;
        setClientForTest(client);
        givenDate(ProxiedEntitiesIngest.DAY);
    }

    @AfterAll
    public static void afterAll() {
        TypeRegistry.reset();
    }

    @Override
    public ShardQueryLogic getLogic() {
        return logic;
    }

    @Override
    public Authorizations getAuths() {
        return new Authorizations(requestedAuths.split(","));
    }

    @Override
    protected Set<Authorizations> getQueryAuths() throws AuthorizationException {
        return AuthorizationsUtil.getDowngradedAuthorizations(requestedAuths, principal, principal);
    }

    @Override
    protected void extraConfigurations() {
        // no-op
    }

    @Override
    protected void extraAssertions() {
        // no-op
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allowedQueries")
    public void returnsOnlyVisibleEvents(String name, DatawavePrincipal chain, String auths, Event... expected) throws Exception {
        principal = chain;
        requestedAuths = auths;
        givenQuery(QUERY);
        expectPlan(QUERY);
        expectResultCount(expected.length);
        if (expected.length > 0) {
            expectUUIDs(Arrays.stream(expected).map(Event::uuid).collect(Collectors.toSet()));
        }
        planAndExecuteQuery();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("overAuthorizedQueries")
    public void rejectsNpeAuthsNotHeldByUser(String name, DatawavePrincipal chain, String auths) {
        principal = chain;
        requestedAuths = auths;
        givenQuery(QUERY);
        assertThrows(AuthorizationException.class, this::planAndExecuteQuery);
    }

    private static Stream<Arguments> allowedQueries() {
        return Stream.of(
                        Arguments.of("one user, one NPE, intersection", chain(USER_ONE, SERVER_ONE), "A,B,C",
                                        new Event[] {Event.COMMON, Event.USER_ONE_SERVER_ONE}),
                        Arguments.of("one user, one NPE, user-only auth requested", chain(USER_ONE, SERVER_ONE), "A,B,C,D",
                                        new Event[] {Event.COMMON, Event.USER_ONE_SERVER_ONE}),
                        Arguments.of("second user, same NPE, intersection", chain(USER_TWO, SERVER_ONE), "A,B,E",
                                        new Event[] {Event.COMMON, Event.USER_TWO_SERVER_ONE}),
                        Arguments.of("second user, same NPE, user-only auth requested", chain(USER_TWO, SERVER_ONE), "A,B,E,F",
                                        new Event[] {Event.COMMON, Event.USER_TWO_SERVER_ONE}),
                        Arguments.of("one user, two NPEs, intersection", chain(USER_ONE, SERVER_ONE, SERVER_TWO), "A,B", new Event[] {Event.COMMON}),
                        Arguments.of("one user, two NPEs, first-only auth requested", chain(USER_ONE, SERVER_ONE, SERVER_TWO), "A,B,C",
                                        new Event[] {Event.COMMON}),
                        Arguments.of("one user, one NPE, no matching record", chain(USER_ONE, SERVER_ONE), "C", new Event[0]));
    }

    private static Stream<Arguments> overAuthorizedQueries() {
        return Stream.of(Arguments.of("one user, one NPE", chain(USER_ONE, SERVER_ONE), "A,B,C,G"),
                        Arguments.of("second user, same NPE", chain(USER_TWO, SERVER_ONE), "A,B,E,G"),
                        Arguments.of("one user, two NPEs", chain(USER_ONE, SERVER_ONE, SERVER_TWO), "A,B,C,G"));
    }

    private static DatawaveUser user(String name, UserType type, String... auths) {
        SubjectIssuerDNPair dn = SubjectIssuerDNPair.of("cn=" + name, "cn=example ca");
        return new DatawaveUser(dn, type, Set.of(auths), null, null, System.currentTimeMillis());
    }

    private static DatawavePrincipal chain(DatawaveUser... users) {
        return new DatawavePrincipal(List.of(users));
    }
}
