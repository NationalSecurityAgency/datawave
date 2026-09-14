package datawave.test.framework.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CombinationTest {

    private static final Logger log = LoggerFactory.getLogger(CombinationTest.class);

    @Test
    public void testSingleton() {
        List<String> elements = Arrays.asList("a");
        List<List<String>> expected = Arrays.asList(Arrays.asList("a"));
        assertCombinations(elements, expected, 1);
    }

    @Test
    public void testSmallPermutation() {
        List<String> elements = Arrays.asList("a", "b");
        //  @formatter:off
        List<List<String>> expected = Arrays.asList(
                Arrays.asList("a"),
                Arrays.asList("b"),
                Arrays.asList("a", "b")
        );
        //  @formatter:on
        assertCombinations(elements, expected, 3);
    }

    @Test
    public void testFullPermutation() {
        List<String> elements = Arrays.asList("i", "ri", "e", "tf");
        //  @formatter:off
        List<List<String>> expected = Arrays.asList(
                Arrays.asList("i"),
                Arrays.asList("ri"),
                Arrays.asList("e"),
                Arrays.asList("tf"),
                Arrays.asList("i", "ri"),
                Arrays.asList("i", "e"),
                Arrays.asList("i", "tf"),
                Arrays.asList("ri", "e"),
                Arrays.asList("ri", "tf"),
                Arrays.asList("e", "tf"),
                Arrays.asList("i", "ri", "e"),
                Arrays.asList("i", "ri", "tf"),
                Arrays.asList("i", "e", "tf"),
                Arrays.asList("ri", "e", "tf"),
                Arrays.asList("i", "ri", "e", "tf")
        );
        //  @formatter:on
        assertCombinations(elements, expected, 15);
    }

    private void assertCombinations(List<String> elements, List<List<String>> expected, int expectedSize) {
        List<List<String>> results = Combination.getAllCombinations(elements);

        expected.sort(Comparator.comparingInt(List::size));
        results.sort(Comparator.comparingInt(List::size));

        printResults(results);

        assertEquals(expectedSize, results.size());
        assertEquals(expected, results);
    }

    private void printResults(List<List<String>> results) {
        for (List<String> result : results) {
            log.info("{}", result);
        }
    }
}
