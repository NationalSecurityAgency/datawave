package datawave.accumulo.inmemory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.client.admin.TimeType;
import org.junit.jupiter.api.Test;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

public class InMemoryAccumuloCacheTest {

    private final InMemoryAccumuloCache instances = new InMemoryAccumuloCache(Caffeine.newBuilder().build());

    @Test
    public void defaultIsShared() {
        assertSame(InMemoryAccumuloCache.getDefault(), InMemoryAccumuloCache.getDefault());
    }

    @Test
    public void sameNameReturnsTheSameInstance() {
        InMemoryAccumulo first = instances.getOrCreate("shared");
        first.createTable("root", "sharedTable", true, TimeType.LOGICAL);

        InMemoryAccumulo second = instances.getOrCreate("shared");
        assertSame(first, second);
        assertTrue(new InMemoryAccumuloClient("root", second).tableOperations().exists("sharedTable"));
    }

    @Test
    public void differentNamesReturnDifferentInstances() {
        assertNotSame(instances.getOrCreate("one"), instances.getOrCreate("two"));
        assertEquals(2, instances.size());
    }

    @Test
    public void removedInstanceIsReplacedWithANewOne() {
        InMemoryAccumulo first = instances.getOrCreate("removed");
        assertSame(first, instances.remove("removed"));
        assertNull(instances.remove("removed"));
        assertNotSame(first, instances.getOrCreate("removed"));
    }

    @Test
    public void clearRemovesEveryInstance() {
        instances.getOrCreate("one");
        instances.getOrCreate("two");
        instances.clear();
        assertEquals(0, instances.size());
    }

    @Test
    public void instancesExpireAfterGoingUnusedForTheConfiguredTime() {
        AtomicLong nanos = new AtomicLong();
        Cache<String,InMemoryAccumulo> cache = Caffeine.newBuilder().expireAfterAccess(Duration.ofMinutes(10)).ticker(nanos::get).build();
        InMemoryAccumuloCache expiring = new InMemoryAccumuloCache(cache);

        InMemoryAccumulo used = expiring.getOrCreate("used");
        InMemoryAccumulo idle = expiring.getOrCreate("idle");

        // Looking up "used" part way through resets its clock, while "idle" keeps aging.
        nanos.addAndGet(TimeUnit.MINUTES.toNanos(6));
        assertSame(used, expiring.getOrCreate("used"));
        nanos.addAndGet(TimeUnit.MINUTES.toNanos(6));

        // size() does not process pending evictions, so do that through the injected cache before counting.
        cache.cleanUp();
        assertEquals(1, expiring.size());
        assertSame(used, expiring.getOrCreate("used"));

        // Once evicted, the next lookup by that name gets a new instance.
        assertNotSame(idle, expiring.getOrCreate("idle"));
    }
}
