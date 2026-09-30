package datawave.microservice.annotationCache.config;

import static datawave.microservice.annotationCache.api.Constants.FETCH_MAP;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import datawave.microservice.annotationCache.AnnotationCacheTopologyListener;

/** Verifies fetch invalidation after a real cluster member failure. */
class AnnotationCacheTopologyListenerClusterIntegrationTest {
    private final List<HazelcastInstance> instances = new ArrayList<>();
    private final List<AnnotationCacheTopologyListener> listeners = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (AnnotationCacheTopologyListener listener : listeners) {
            try {
                listener.close();
            } catch (RuntimeException e) {
                // A listener on the member terminated by the test cannot unregister its callbacks.
            }
        }
        for (HazelcastInstance instance : instances) {
            if (instance.getLifecycleService().isRunning()) {
                instance.shutdown();
            }
        }
    }

    @Test
    void survivingMemberInvalidatesFetchRecordsAfterMemberLoss() throws Exception {
        String clusterName = "annotation-topology-cluster-" + UUID.randomUUID();
        int[] ports = availablePorts();
        AnnotationCacheProperties properties = new AnnotationCacheProperties();
        properties.setTopologySettleDelay(Duration.ZERO);

        HazelcastInstance first = Hazelcast.newHazelcastInstance(clusterConfig(clusterName, ports[0], ports));
        instances.add(first);
        AnnotationCacheTopologyListener firstListener = new AnnotationCacheTopologyListener(first, properties);
        listeners.add(firstListener);

        HazelcastInstance second = Hazelcast.newHazelcastInstance(clusterConfig(clusterName, ports[1], ports));
        instances.add(second);
        AnnotationCacheTopologyListener secondListener = new AnnotationCacheTopologyListener(second, properties);
        listeners.add(secondListener);

        await("members to form a cluster", () -> first.getCluster().getMembers().size() == 2 && second.getCluster().getMembers().size() == 2
                        && first.getPartitionService().isClusterSafe() && second.getPartitionService().isClusterSafe());

        IMap<String,String> fetch = second.getMap(FETCH_MAP + "UUID:document");
        fetch.put("auth", "record");
        first.getLifecycleService().terminate();
        await("surviving member to become safe", () -> second.getCluster().getMembers().size() == 1 && second.getPartitionService().isClusterSafe());

        secondListener.poll();

        assertTrue(fetch.isEmpty());
    }

    private Config clusterConfig(String clusterName, int port, int[] memberPorts) {
        Config config = new Config();
        config.setClusterName(clusterName);
        config.setProperty("hazelcast.logging.type", "none");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().setPort(port).setPortAutoIncrement(false);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getAutoDetectionConfig().setEnabled(false);
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true);
        for (int memberPort : memberPorts) {
            join.getTcpIpConfig().addMember("127.0.0.1:" + memberPort);
        }
        return config;
    }

    private int[] availablePorts() throws IOException {
        try (ServerSocket first = new ServerSocket(0); ServerSocket second = new ServerSocket(0)) {
            return new int[] {first.getLocalPort(), second.getLocalPort()};
        }
    }

    private void await(String description, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(25);
        }
        throw new AssertionError("Timed out waiting for " + description);
    }
}
