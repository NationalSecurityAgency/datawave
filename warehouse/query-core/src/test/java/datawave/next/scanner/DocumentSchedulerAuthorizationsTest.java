package datawave.next.scanner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.Test;

import datawave.microservice.query.QueryImpl;
import datawave.query.config.ShardQueryConfiguration;

/**
 * Verifies that the {@link DocumentScheduler} preserves the full set of authorizations for the calling entity chain.
 * <p>
 * Every set produced by {@link datawave.security.util.AuthorizationsMinimizer#minimize(java.util.Collection)} must reach the scanner. The first set is applied
 * to the scanner itself and each remaining set becomes a visibility filter, which is what restricts results to data visible to every entity in the chain.
 */
public class DocumentSchedulerAuthorizationsTest {

    /**
     * Two authorization sets where neither is a subset of the other, so minimization cannot reduce them to one.
     */
    private static final Authorizations USER_AUTHS = new Authorizations("A", "B", "ORG1");
    private static final Authorizations SERVER_AUTHS = new Authorizations("A", "C", "ORG2");

    @Test
    public void testMultipleAuthorizationSetsSurviveSchedulerConstruction() {
        Set<Authorizations> auths = Set.of(USER_AUTHS, SERVER_AUTHS);
        ShardQueryConfiguration config = createConfig(auths);

        new DocumentScheduler(config);

        Set<Authorizations> applied = config.getDocumentScannerConfig().getAuthorizations();
        assertEquals(2, applied.size(), "both authorization sets must reach the scanner");
        assertTrue(applied.contains(USER_AUTHS));
        assertTrue(applied.contains(SERVER_AUTHS));
    }

    @Test
    public void testSingleAuthorizationSetIsPreserved() {
        Set<Authorizations> auths = Set.of(USER_AUTHS);
        ShardQueryConfiguration config = createConfig(auths);

        new DocumentScheduler(config);

        Set<Authorizations> applied = config.getDocumentScannerConfig().getAuthorizations();
        assertEquals(1, applied.size());
        assertTrue(applied.contains(USER_AUTHS));
    }

    private ShardQueryConfiguration createConfig(Set<Authorizations> auths) {
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setDocumentScannerConfig(new DocumentScannerConfig());
        config.setAuthorizations(auths);
        config.setQueries(List.of());

        QueryImpl query = new QueryImpl();
        query.setId(UUID.randomUUID());
        config.setQuery(query);

        return config;
    }
}
