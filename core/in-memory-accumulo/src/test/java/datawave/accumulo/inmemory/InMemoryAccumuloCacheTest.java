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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.github.benmanes.caffeine.cache.Cache;

public class InMemoryAccumuloCacheTest {

    @AfterEach
    public void clearCache() {
        InMemoryAccumuloCache.clear();
    }

    @Test
    public void sameNameReturnsTheSameInstance() {
        InMemoryAccumulo first = InMemoryAccumuloCache.getOrCreate("shared");
        first.createTable("root", "sharedTable", true, TimeType.LOGICAL);

        InMemoryAccumulo second = InMemoryAccumuloCache.getOrCreate("shared");
        assertSame(first, second);
        assertTrue(new InMemoryAccumuloClient("root", second).tableOperations().exists("sharedTable"));
    }

    @Test
    public void differentNamesReturnDifferentInstances() {
        assertNotSame(InMemoryAccumuloCache.getOrCreate("one"), InMemoryAccumuloCache.getOrCreate("two"));
        assertEquals(2, InMemoryAccumuloCache.size());
    }

    @Test
    public void removedInstanceIsReplacedWithANewOne() {
        InMemoryAccumulo first = InMemoryAccumuloCache.getOrCreate("removed");
        assertSame(first, InMemoryAccumuloCache.remove("removed"));
        assertNull(InMemoryAccumuloCache.remove("removed"));
        assertNotSame(first, InMemoryAccumuloCache.getOrCreate("removed"));
    }

    @Test
    public void clearRemovesEveryInstance() {
        InMemoryAccumuloCache.getOrCreate("one");
        InMemoryAccumuloCache.getOrCreate("two");
        InMemoryAccumuloCache.clear();
        assertEquals(0, InMemoryAccumuloCache.size());
    }

    @Test
    public void instancesExpireAfterGoingUnusedForTheConfiguredTime() {
        AtomicLong nanos = new AtomicLong();
        Cache<String,InMemoryAccumulo> cache = InMemoryAccumuloCache.newCache(Duration.ofMinutes(10), nanos::get);

        InMemoryAccumulo used = cache.get("used", InMemoryAccumulo::new);
        InMemoryAccumulo idle = cache.get("idle", InMemoryAccumulo::new);

        // Looking up "used" part way through resets its clock, while "idle" keeps aging.
        nanos.addAndGet(TimeUnit.MINUTES.toNanos(6));
        assertSame(used, cache.getIfPresent("used"));
        nanos.addAndGet(TimeUnit.MINUTES.toNanos(6));

        assertSame(used, cache.getIfPresent("used"));
        assertNull(cache.getIfPresent("idle"));
        cache.cleanUp();
        assertEquals(1, cache.estimatedSize());

        // Once evicted, the next lookup by that name gets a new instance.
        assertNotSame(idle, cache.get("idle", InMemoryAccumulo::new));
    }
}
