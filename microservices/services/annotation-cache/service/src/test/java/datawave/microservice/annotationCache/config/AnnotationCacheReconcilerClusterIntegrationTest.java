package datawave.microservice.annotationCache.config;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.DOC_ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.FETCH_MAP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import datawave.annotation.protobuf.v1.AnnotationMessage;
import datawave.microservice.annotationCache.AnnotationCacheReconciler;

/** Exercises reconciliation after abrupt member loss in a real two-member Hazelcast cluster. */
class AnnotationCacheReconcilerClusterIntegrationTest {
    private final List<HazelcastInstance> instances = new ArrayList<>();
    private final List<AnnotationCacheReconciler> reconcilers = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (AnnotationCacheReconciler reconciler : reconcilers) {
            try {
                reconciler.close();
            } catch (RuntimeException e) {
                // The reconciler on the member terminated by the test cannot unregister its listeners.
            }
        }
        for (HazelcastInstance instance : instances) {
            if (instance.getLifecycleService().isRunning()) {
                instance.shutdown();
            }
        }
    }

    @Test
    void survivingMemberRepairsDynamicallyDiscoveredMapAfterMemberFailure() throws Exception {
        String clusterName = "annotation-reconciliation-cluster-" + UUID.randomUUID();
        int[] ports = availablePorts();
        AnnotationCacheProperties properties = properties();

        HazelcastInstance first = Hazelcast.newHazelcastInstance(clusterConfig(clusterName, ports[0], ports));
        instances.add(first);
        AnnotationCacheReconciler firstReconciler = new AnnotationCacheReconciler(first, properties);
        reconcilers.add(firstReconciler);

        HazelcastInstance second = Hazelcast.newHazelcastInstance(clusterConfig(clusterName, ports[1], ports));
        instances.add(second);
        AnnotationCacheReconciler secondReconciler = new AnnotationCacheReconciler(second, properties);
        reconcilers.add(secondReconciler);

        await("members to form a cluster", () -> first.getCluster().getMembers().size() == 2 && second.getCluster().getMembers().size() == 2
                        && first.getPartitionService().isClusterSafe() && second.getPartitionService().isClusterSafe());

        // Consume initial startup requests and establish a recent completion so the later pass is caused by member removal rather than the periodic interval.
        firstReconciler.poll();
        secondReconciler.poll();
        assertNotNull(second.<String,Long> getMap("annotation-cache-reconciliation-control").get("last-completed-at"));

        String cacheKey = "uuid:document";
        String annotationId = "annotation";
        // Create this map after both reconcilers registered their distributed-object listeners. The survivor must discover it through the listener.
        first.<String,AnnotationMessage> getMap(ANNOTATIONS_MAP + cacheKey).put(annotationId, AnnotationMessage.getDefaultInstance());
        second.<String,String> getMap(FETCH_MAP + cacheKey).put("auth", "record");
        assertTrue(second.<String,Set<String>> getMap(DOC_ANNOTATIONS_MAP).isEmpty());

        first.getLifecycleService().terminate();
        await("surviving member to promote backups and become safe",
                        () -> second.getCluster().getMembers().size() == 1 && second.getPartitionService().isClusterSafe()
                                        && second.<String,AnnotationMessage> getMap(ANNOTATIONS_MAP + cacheKey).containsKey(annotationId));

        secondReconciler.poll();

        assertEquals(Set.of(annotationId), second.<String,Set<String>> getMap(DOC_ANNOTATIONS_MAP).get(cacheKey));
        assertTrue(second.getMap(FETCH_MAP + cacheKey).isEmpty());
    }

    private AnnotationCacheProperties properties() {
        AnnotationCacheProperties properties = new AnnotationCacheProperties();
        properties.setReconciliationInterval(Duration.ofHours(1));
        properties.setReconciliationSettleDelay(Duration.ZERO);
        properties.setReconciliationMaxMapsPerRun(100);
        return properties;
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
