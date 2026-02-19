package datawave.next;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.security.SecureRandom;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.Test;

public class StochasticScanResultTest {

    private final int bound = 10;
    private final int maxIterations = 1_000;
    private final SecureRandom random = new SecureRandom();

    @Test
    public void testIntersections() {
        for (int i = 0; i < maxIterations; i++) {
            SortedSet<Key> leftKeys = create(bound);
            ScanResult left = new ScanResult();
            left.addKeys(leftKeys);
            assertResultKeys(left, leftKeys);

            SortedSet<Key> rightKeys = create(bound);
            ScanResult right = new ScanResult();
            right.addKeys(rightKeys);
            assertResultKeys(right, rightKeys);

            SortedSet<Key> intersectedKeys = new TreeSet<>();
            intersectedKeys.addAll(leftKeys);
            intersectedKeys.retainAll(rightKeys);

            left.intersect(right);
            assertResultKeys(left, intersectedKeys);
        }
    }

    @Test
    public void testUnions() {
        for (int i = 0; i < maxIterations; i++) {
            SortedSet<Key> leftKeys = create(bound);
            ScanResult left = new ScanResult();
            left.addKeys(leftKeys);
            assertResultKeys(left, leftKeys);

            SortedSet<Key> rightKeys = create(bound);
            ScanResult right = new ScanResult();
            right.addKeys(rightKeys);
            assertResultKeys(right, rightKeys);

            SortedSet<Key> unionKeys = new TreeSet<>();
            unionKeys.addAll(leftKeys);
            unionKeys.addAll(rightKeys);

            left.union(right);
            assertResultKeys(left, unionKeys);
        }
    }

    private void assertResultKeys(ScanResult result, SortedSet<Key> keys) {
        assertEquals(keys.size(), result.getResults().size());
        if (!keys.isEmpty()) {
            assertEquals(keys.first(), result.getMin());
            assertEquals(keys.last(), result.getMax());
        }
    }

    private SortedSet<Key> create(int bound) {
        int max = random.nextInt(bound);
        SortedSet<Key> keys = new TreeSet<>();
        while (keys.size() < max) {
            int index = random.nextInt(10);
            Key key = createKey(index);
            keys.add(key);
        }
        return keys;
    }

    private Key createKey(int i) {
        return new Key("row", "datatype\0uid-" + i);
    }
}
