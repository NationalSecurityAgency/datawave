package datawave.query.tables;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BitSetIteratorTest {

    private final List<Integer> data = new ArrayList<>();
    private final List<Integer> expected = new ArrayList<>();
    private final List<Integer> results = new ArrayList<>();

    @BeforeEach
    public void beforeEach() {
        data.clear();
        expected.clear();
        results.clear();
    }

    @Test
    public void testZeroIteration() {
        data(0);
        expect(0);
        iterate();
    }

    @Test
    public void testEvenIteration() {
        data(0, 2, 4, 6, 8);
        expect(0, 2, 4, 6, 8);
        iterate();
    }

    @Test
    public void testOddIteration() {
        data(1, 3, 5, 7, 9);
        expect(1, 3, 5, 7, 9);
        iterate();
    }

    private void iterate() {
        BitSet bitset = new BitSet();
        for (int n : data) {
            bitset.set(n);
        }

        BitSetIterator iter = new BitSetIterator(bitset);
        while (iter.hasNext()) {
            int result = iter.next();
            results.add(result);
        }

        assertEquals(expected, results);
    }

    private void data(int... numbers) {
        for (int number : numbers) {
            data.add(number);
        }
    }

    private void expect(int... numbers) {
        for (int number : numbers) {
            expected.add(number);
        }
    }

    private BitSet create(int... offsets) {
        BitSet bitset = new BitSet();
        for (int offset : offsets) {
            bitset.set(offset);
        }
        return bitset;
    }
}
