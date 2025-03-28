package datawave.data.normalizer;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NumberNormalizerTest {
    
    private final NumberNormalizer normalizer = new NumberNormalizer();
    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    
    /**
     * Verify that the equivalent numbers 1 and 1.00000000 are normalized to the same encoding.
     */
    @Test
    public void testNormalizingEquivalentWholeAndDecimalNumber() {
        String expected = "+aE1";
        assertNormalizeResult("1", expected);
        assertNormalizeResult("1.00000000", expected);
    }
    
    /**
     * Verify that a negative number is normalized to the correct encoding.
     */
    @Test
    public void testNormalizingNegativeDecimal() {
        String expected = "!ZE9";
        assertNormalizeResult("-1.0", expected);
        
        assertComparativelyConsecutive(expected, normalizer.normalize("1.0"));
    }
    
    /**
     * Verify that three different numbers are normalized to their correct respective encodings, and also verify that their encodings evaluate to the same
     * consecutive order as the original numbers.
     */
    @Test
    public void testNormalizingNegativeToPositiveRangeInThousandths() {
        String expected1 = "!dE9";
        assertNormalizeResult("-0.0001", expected1);
        
        String expected2 = "+AE0";
        assertNormalizeResult("0", expected2);
        
        String expected3 = "+VE1";
        assertNormalizeResult("0.00001", expected3);
        
        assertComparativelyConsecutive(expected1, expected2, expected3);
    }
    
    /**
     * Verify that large numbers are correctly normalized, and that their encodings evaluate to the same consecutive order as the original numbers.
     */
    @Test
    public void testNormalizingMaxIntegerValue() {
        String expected1 = "+jE2.147483647";
        assertNormalizeResult(Integer.toString(Integer.MAX_VALUE), "+jE2.147483647");
        
        String expected2 = "+jE2.147483646";
        assertNormalizeResult(Integer.toString(Integer.MAX_VALUE - 1), "+jE2.147483646");
        
        assertComparativelyConsecutive(expected2, expected1);
    }
    
    /**
     * Verify that two numbers that are equal, but one with extra zeroes, are normalized to the same encoding.
     */
    @Test
    public void testNormalizingEqualNumbersWithExtraZeroes() {
        String expected = "!dE1";
        assertNormalizeResult("-0.0009", expected);
        assertNormalizeResult("-0.00090", expected);
    }
    
    /**
     * Verify that different forms of zero will normalize to the same encoding.
     */
    @Test
    public void testNormalizingEquivalentZeroes() {
        assertNormalizeResult("-0.0", "+AE0");
        assertNormalizeResult("0", "+AE0");
        assertNormalizeResult("0.0", "+AE0");
    }
    
    private void assertNormalizeResult(String input, String expected) {
        assertEquals(normalizer.normalize(input), expected);
    }
    
    private void assertComparativelyConsecutive(String... values) {
        for (int i = 0; i < values.length - 1; i++) {
            int compare = values[i].compareTo(values[i + 1]);
            if (compare > 0) {
                Assertions.fail("Expected values to be consecutive, but encountered " + values[i] + " which is greater than " + values[i + 1]);
            }
        }
    }
    
    /**
     * Generate random numbers and corresponding regex patterns, and verify that the patterns match against the numbers, and that the corresponding normalized
     * regex patterns match against the corresponding normalized numbers.
     */
    @Test
    void testRandomRegexPatterns() {
        for (int i = 0; i < 1000; i++) {
            // Get a random number. Call getFastRandomNumber() for a quick test that takes less than a minute to complete. Call getRandomNumber() to get numbers
            // that are random across a much larger scale, but expect the test to take possibly more than 20 minutes to complete.
            String num = getFastRandomNumber();
            String normalizedNum = normalizer.normalize(num);
            
            // Generate 100 random patterns that should match against the number.
            for (int j = 0; j < 100; j++) {
                StringBuilder pattern = new StringBuilder();
                
                int startPos = 0;
                // Randomly start the regex with .*
                if (random.nextBoolean()) {
                    pattern.append(".*");
                    // If the number originally started with a '-', skip over it when appending to the pattern.
                    if (num.charAt(0) == '-') {
                        startPos = 1;
                    }
                }
                
                boolean seenDecimal = false;
                for (int pos = startPos; pos < num.length(); pos++) {
                    char character = num.charAt(pos);
                    if (Character.isDigit(character)) {
                        if (random.nextBoolean()) {
                            Set<Integer> candidates = new HashSet<>();
                            for (int count = 0; count < 10; count++) {
                                if (random.nextBoolean()) {
                                    candidates.add(random.nextInt(10));
                                }
                            }
                            candidates.add(Integer.valueOf(String.valueOf(character)));
                            pattern.append('[');
                            candidates.forEach(pattern::append);
                            pattern.append(']');
                        } else if (random.nextBoolean()) {
                            pattern.append('.');
                        } else if (random.nextBoolean()) {
                            pattern.append("\\d");
                        } else {
                            pattern.append(character);
                        }
                        if (random.nextBoolean()) {
                            pattern.append("*");
                        } else if (random.nextBoolean()) {
                            pattern.append("+");
                        } else if (random.nextBoolean()) {
                            pattern.append("{1,3}");
                        }
                    } else if (character == '.') {
                        seenDecimal = true;
                        pattern.append("\\.");
                    } else {
                        pattern.append(character);
                    }
                }
                
                // If we've seen a decimal point, randomly append a trailing .*
                if (seenDecimal && random.nextBoolean()) {
                    pattern.append(".*");
                }
                
                // Verify the pattern matches the original number.
                assertThat(Pattern.compile(pattern.toString()).matcher(num).matches()).as("matching \n\"" + pattern + "\"\n to " + num).isTrue();
                
                // Normalize the pattern.
                String normalizedPattern = normalizer.normalizeRegex(pattern.toString());
                
                // check the normalized match
                assertThat(Pattern.compile(normalizedPattern).matcher(normalizedNum).matches())
                                .as("matching \n\"" + pattern + "\" -> \n\"" + normalizedPattern + "\"\n to " + num + " -> " + normalizedNum).isTrue();
                
                // reormalize the pattern.
                String renormalizedPattern = normalizer.normalizeRegex(normalizedPattern);
                assertEquals(renormalizedPattern, normalizedPattern);
            }
        }
    }
    
    /**
     * Return a random number that when used in {@link #testRandomRegexPatterns()}, will not make the test take more than a minute to complete.
     * 
     * @return a random number
     */
    private String getFastRandomNumber() {
        String num = Double.toString(random.nextDouble());
        if (num.contains("E")) {
            num = Double.toString(random.nextDouble());
        }
        return num;
    }
    
    /**
     * Return a random number. Note: when used in {@link #testRandomRegexPatterns()}, the test can take more than 20 minutes to complete.
     * 
     * @return a random number
     */
    private String getRandomNumber() {
        return random.nextBoolean() ? getRandomNumberLessThanZero() : getRandomNumberGreaterThanZero();
    }
    
    /**
     * Return a random number that is larger than zero, randomly negative, and randomly whole.
     * 
     * @return a random number
     */
    private String getRandomNumberGreaterThanZero() {
        BigDecimal decimal = getRandomBigDecimal();
        
        // Move the decimal point to the right randomly.
        int leadingZeros = random.nextInt(0, 26);
        decimal = decimal.movePointRight(leadingZeros);
        
        // Randomly trim the mantissa to make the number whole.
        if (random.nextBoolean()) {
            decimal = decimal.setScale(0, RoundingMode.FLOOR);
        }
        
        return decimal.toPlainString();
    }
    
    /**
     * Return a random number that is less than zero, and randomly negative.
     * 
     * @return a random number
     */
    private String getRandomNumberLessThanZero() {
        BigDecimal decimal = getRandomBigDecimal();
        
        // Move the decimal point to the left randomly.
        int leadingZeros = random.nextInt(0, 26);
        decimal = decimal.movePointLeft(leadingZeros);
        
        // Limit the mantissa length.
        decimal = decimal.setScale(26, RoundingMode.FLOOR);
        
        return decimal.toPlainString();
    }
    
    /**
     * Return a random big decimal that is randomly negative.
     * 
     * @return a new big decimal
     */
    private BigDecimal getRandomBigDecimal() {
        BigDecimal decimal = BigDecimal.valueOf(random.nextDouble());
        if (random.nextBoolean()) {
            decimal = decimal.negate();
        }
        return decimal;
    }
}
