package datawave.microservice.querymetric;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collection;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;

import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;

public class HazelcastUtils {
    
    private static final int ASSERT_TRUE_EVENTUALLY_TIMEOUT = 300;
    
    public static class CountdownLatchAdapter implements Latch {
        
        private final CountDownLatch latch;
        
        CountdownLatchAdapter(CountDownLatch latch) {
            this.latch = latch;
        }
        
        @Override
        public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            return latch.await(timeout, unit);
        }
        
        @Override
        public long getCount() {
            return latch.getCount();
        }
    }
    
    public static interface Latch {
        
        boolean await(long timeout, TimeUnit unit) throws InterruptedException;
        
        long getCount();
    }
    
    private static abstract class AssertTask {
        
        public abstract void run() throws Exception;
    }
    
    public static String randomMapName() {
        return UuidUtil.newUnsecureUuidString();
    }
    
    public static String generateRandomString(int length) {
        StringBuilder sb = new StringBuilder(length);
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            char character = (char) (random.nextInt(26) + 'a');
            sb.append(character);
        }
        return sb.toString();
    }
    
    public static String randomString() {
        return UuidUtil.newUnsecureUuidString();
    }
    
    public static void sleepAtLeastMillis(long sleepFor) {
        boolean interrupted = false;
        try {
            long remainingNanos = MILLISECONDS.toNanos(sleepFor);
            long sleepUntil = System.nanoTime() + remainingNanos;
            while (remainingNanos > 0) {
                try {
                    NANOSECONDS.sleep(remainingNanos);
                } catch (InterruptedException e) {
                    interrupted = true;
                } finally {
                    remainingNanos = sleepUntil - System.nanoTime();
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
    
    public static void assertClusterSize(int expectedSize, HazelcastInstance... instances) {
        for (int i = 0; i < instances.length; i++) {
            int clusterSize = getClusterSize(instances[i]);
            if (expectedSize != clusterSize) {
                fail(String.format("Cluster size is not correct. Expected: %d, actual: %d, instance index: %d", expectedSize, clusterSize, i));
            }
        }
    }
    
    public static int getClusterSize(HazelcastInstance instance) {
        Set<Member> members = instance.getCluster().getMembers();
        return members == null ? 0 : members.size();
    }
    
    public static void assertClusterSizeEventually(int expectedSize, HazelcastInstance... instances) {
        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(expectedSize, instance, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
        }
    }
    
    public static void assertClusterSizeEventually(int expectedSize, Collection<HazelcastInstance> instances) {
        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(expectedSize, instance, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
        }
    }
    
    public static void assertClusterSizeEventually(final int expectedSize, final HazelcastInstance instance, long timeoutSeconds) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertClusterSize(expectedSize, instance);
            }
        }, timeoutSeconds);
    }
    
    public static void assertTrueEventually(String message, AssertTask task, long timeoutSeconds) {
        AssertionError error = null;
        // we are going to check five times a second
        int sleepMillis = 200;
        long iterations = timeoutSeconds * 5;
        for (int i = 0; i < iterations; i++) {
            try {
                try {
                    task.run();
                } catch (Exception e) {
                    throw rethrow(e);
                }
                return;
            } catch (AssertionError e) {
                error = e;
            }
            sleepMillis(sleepMillis);
        }
        if (error != null) {
            throw error;
        }
        fail("assertTrueEventually() failed without AssertionError! " + message);
    }
    
    public static void assertTrueEventually(AssertTask task, long timeoutSeconds) {
        assertTrueEventually(null, task, timeoutSeconds);
    }
    
    public static void assertTrueEventually(String message, AssertTask task) {
        assertTrueEventually(message, task, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }
    
    public static void assertTrueEventually(AssertTask task) {
        assertTrueEventually(null, task, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }
    
    public static void assertOpenEventually(CountDownLatch latch) {
        assertOpenEventually(new CountdownLatchAdapter(latch), ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }
    
    public static void assertOpenEventually(Latch latch, long timeoutSeconds) {
        try {
            boolean completed = latch.await(timeoutSeconds, TimeUnit.SECONDS);
            assertTrue(completed, String.format("failed to complete within %d seconds, count left: %d", timeoutSeconds, latch.getCount()));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static String generateKeyOwnedBy(HazelcastInstance instance) {
        Cluster cluster = instance.getCluster();
        checkMemberCount(true, cluster);
        checkPartitionCountGreaterOrEqualMemberCount(instance);
        
        Member localMember = cluster.getLocalMember();
        PartitionService partitionService = instance.getPartitionService();
        while (true) {
            String id = randomString();
            Partition partition = partitionService.getPartition(id);
            if (comparePartitionOwnership(true, localMember, partition)) {
                return id;
            }
        }
    }
    
    public static void checkPartitionCountGreaterOrEqualMemberCount(HazelcastInstance instance) {
        Cluster cluster = instance.getCluster();
        int memberCount = cluster.getMembers().size();
        
        InternalPartitionService internalPartitionService = getPartitionService(instance);
        int partitionCount = internalPartitionService.getPartitionCount();
        
        if (partitionCount < memberCount) {
            throw new UnsupportedOperationException("Partition count should be equal or greater than member count!");
        }
    }
    
    public static InternalPartitionService getPartitionService(HazelcastInstance hz) {
        return getNode(hz).getPartitionService();
    }
    
    public static boolean comparePartitionOwnership(boolean ownedBy, Member member, Partition partition) {
        Member owner = partition.getOwner();
        if (ownedBy) {
            return member.equals(owner);
        } else {
            return !member.equals(owner);
        }
    }
    
    public static void checkMemberCount(boolean generateOwnedKey, Cluster cluster) {
        if (generateOwnedKey) {
            return;
        }
        Set<Member> members = cluster.getMembers();
        if (members.size() < 2) {
            throw new UnsupportedOperationException("Cluster has only one member, you can not generate a `not owned key`");
        }
    }
    
    public static void sleepMillis(int millis) {
        try {
            MILLISECONDS.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    public static void warmUpPartition(HazelcastInstance instance) {
        if (instance == null) {
            return;
        }
        PartitionService ps = instance.getPartitionService();
        for (Partition partition : ps.getPartitions()) {
            while (partition.getOwner() == null) {
                sleepMillis(10);
            }
        }
    }
    
    public static void closeConnectionBetween(HazelcastInstance h1, HazelcastInstance h2) {
        if (h1 == null || h2 == null) {
            return;
        }
        Node n1 = getNode(h1);
        Node n2 = getNode(h2);
        suspectMember(n1, n2);
        suspectMember(n2, n1);
    }
    
    public static void suspectMember(HazelcastInstance source, HazelcastInstance target) {
        suspectMember(getNode(source), getNode(target));
    }
    
    public static void suspectMember(Node suspectingNode, Node suspectedNode) {
        suspectMember(suspectingNode, suspectedNode, null);
    }
    
    public static void suspectMember(Node suspectingNode, Node suspectedNode, String reason) {
        if (suspectingNode != null && suspectedNode != null) {
            ClusterServiceImpl clusterService = suspectingNode.getClusterService();
            Member suspectedMember = clusterService.getMember(suspectedNode.getLocalMember().getAddress());
            if (suspectedMember != null) {
                clusterService.suspectMember(suspectedMember, reason, true);
            }
        }
    }
    
    public static Node getNode(HazelcastInstance hz) {
        HazelcastInstanceImpl hazelcastInstanceImpl = getHazelcastInstanceImpl(hz);
        return hazelcastInstanceImpl.node;
    }
    
    /**
     * Retrieves the {@link HazelcastInstanceImpl} from a given Hazelcast instance.
     *
     * @param hz
     *            the Hazelcast instance to retrieve the implementation from
     * @return the {@link HazelcastInstanceImpl} of the given Hazelcast instance
     * @throws IllegalArgumentException
     *             if the {@link HazelcastInstanceImpl} could not be retrieved, e.g. when called with a Hazelcast client instance
     */
    public static HazelcastInstanceImpl getHazelcastInstanceImpl(HazelcastInstance hz) {
        if (hz instanceof HazelcastInstanceImpl) {
            return (HazelcastInstanceImpl) hz;
        } else if (hz instanceof HazelcastInstanceProxy) {
            HazelcastInstanceProxy proxy = (HazelcastInstanceProxy) hz;
            if (proxy.getOriginal() != null) {
                return proxy.getOriginal();
            }
        }
        Class<? extends HazelcastInstance> clazz = hz.getClass();
        throw new IllegalArgumentException("The given HazelcastInstance is not an active HazelcastInstanceImpl: " + clazz);
    }
}
