package datawave.webservice.query.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import java.io.IOException;

import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.webservice.query.limit.QueryHeartbeat;

class QueryHeartbeatCacheTest {

    QueryHeartbeatCache cache;

    @BeforeEach
    void setUp() {
        cache = new QueryHeartbeatCache();
        cache.init();
    }

    /**
     * Verify that adding and retrieving a heartbeat by query ID works.
     */
    @Test
    void testPut() {
        QueryHeartbeat heartbeat = EasyMock.createMock(QueryHeartbeat.class);
        EasyMock.replay(heartbeat);

        cache.put("queryId", heartbeat);
        assertThat(cache.get("queryId")).isEqualTo(heartbeat);
    }

    /**
     * Verify that when stopping and removing a heartbeat, the heartbeat is stopped and no longer present in the cache.
     */
    @Test
    void testStopAndRemoveHeartbeatWithMatch() throws IOException {
        QueryHeartbeat heartbeat = EasyMock.createMock(QueryHeartbeat.class);
        heartbeat.stop();
        EasyMock.expectLastCall();

        EasyMock.replay(heartbeat);

        cache.put("queryId", heartbeat);
        cache.stopAndRemoveHeartbeat("queryId");

        assertThat(cache.get("queryId")).isNull();
        EasyMock.verify(heartbeat);
    }

    /**
     * Verify that when stopping a heartbeat for a mapping that does not exist, an exception is not thrown.
     */
    @Test
    void testStopAndRemoveHeartbeatWithoutMatch() {
        assertThatNoException().isThrownBy(() -> cache.stopAndRemoveHeartbeat("queryId"));
    }
}
