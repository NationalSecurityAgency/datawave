package datawave.data.type.util;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Provides a one-to-one mapping between an input decimal number and a lexicographically sorted index for that number. The index is composed of two parts,
 * roughly derived from scientific notation: the two digit exponential bin and the mantissa, with 'E' as a separator. Thus, an index takes this format:
 * {@code 'bin'E'mantissa'}.
 * <p>
 * The bins are broken into four groups:
 * <ol>
 * <li>!A through !Z represent negative numbers with magnitude greater than one (exponents 25 through 0, respectively)</li>
 * <li>!a through !z represent negative numbers with magnitude less than 1 (exponents -1 through -26, respectively)</li>
 * <li>+A through +Z represent positive numbers with magnitude less than 1 (exponents -26 through -1, respectively)</li>
 * <li>+a through +z represent positive numbers with magnitude greater than one (exponents 0 through 25, respectively)</li>
 * </ol>
 * For positive numbers, the mantissa exactly matches the mantissa of scientific notation. For negative numbers, the mantissa equals ten minus the mantissa of
 * scientific notation.
 * <p>
 * Some example inputs and encodings:
 * <ul>
 * <li>-12344984165 becomes !PE8.7655015835</li>
 * <li>-500 becomes !XE5</li>
 * <li>-0.501 becomes !aE4.99</li>
 * <li>0 becomes +AE0</li>
 * <li>9E-9 becomes +RE9</li>
 * <li>0.501 becomes +ZE5.01</li>
 * <li>10000 becomes +eE1</li>
 * </ul>
 */
public class NumericalEncoder {
    
    private static Map<String,String> positiveNumsEncodeToIntExponentsMap;
    private static Map<String,String> positiveNumsIntToEncodeExponentsMap;
    private static Map<String,String> negativeNumEncodeToIntExponentsMap;
    private static Map<String,String> negativeNumIntToEncodeExponentsMap;
    private static final NumberFormat plainFormatter = new DecimalFormat("0.#########################################################");
    private static final NumberFormat scientificFormatter = new DecimalFormat("0.#########################################################E0");
    private static final String zero = "+AE0";
    private static final List<String> uppercaseLetters = createLetterList('A', 'Z');
    private static final List<String> lowercaseLetters = createLetterList('a', 'z');
    private static final String encodedRegex = "(\\!|\\+)[a-zA-Z][E|e][0-9].?[0-9]*";
    private static final Pattern encodedPattern = Pattern.compile(encodedRegex);
    
    static {
        initNegativeExponents();
        initPositiveExponents();
    }
    
    /**
     * Return an unmodifiable list of letters in order from the given starting letter to the given ending letter.
     * 
     * @param start
     *            the starting letter
     * @param end
     *            the ending letter
     * @return a list of letters
     */
    private static List<String> createLetterList(char start, char end) {
        // @formatter:off
        return Collections.unmodifiableList(
                        IntStream.rangeClosed(start, end)
                                        .mapToObj(c -> "" + (char) c)
                                        .collect(Collectors.toList()));
        // @formatter:on
    }
    
    public static String encode(String input) {
        try {
            BigDecimal decimal = new BigDecimal(input);
            String encodedExponent;
            String mantissa;
            if (decimal.compareTo(BigDecimal.ZERO) == 0) {
                return zero;
            } else if (decimal.compareTo(BigDecimal.ZERO) > 0) {
                // Positive
                String decString = scientificFormatter.format(decimal);
                String[] decParts = decString.split("E");
                mantissa = decParts[0];
                String exp = decParts[1];
                encodedExponent = positiveNumsIntToEncodeExponentsMap.get(exp);
            } else {
                // Negative
                String decString = scientificFormatter.format(decimal);
                String[] decParts = decString.split("E");
                mantissa = decParts[0];
                String exp = decParts[1];
                encodedExponent = negativeNumIntToEncodeExponentsMap.get(exp);
                BigDecimal bigDecMantissa = new BigDecimal(mantissa);
                bigDecMantissa = BigDecimal.TEN.add(bigDecMantissa);
                mantissa = plainFormatter.format(bigDecMantissa);
                
            }
            
            if (encodedExponent == null) {
                throw new NumberFormatException("Exponent exceeded allowed range.");
            }
            
            return encodedExponent + "E" + mantissa;
        } catch (Exception ex) {
            throw new IllegalArgumentException("Error formatting input: " + input + " . Error: " + ex, ex);
        }
    }
    
    /**
     * This provides a quick test that will determine whether this value is possibly encoded. Provides a mechanism that is significantly faster than waiting for
     * the decode method to throw an exception.
     * 
     * @param input
     *            the value to test for encoding
     * @return true if possibly encoded, false if definitely not encoded
     */
    public static boolean isPossiblyEncoded(String input) {
        if (null == input || input.isEmpty())
            return false;
        
        return encodedPattern.matcher(input).matches();
    }
    
    public static BigDecimal decode(String input) {
        BigDecimal output;
        if (input.equals(zero)) {
            return BigDecimal.ZERO;
        } else {
            try {
                String exp = input.substring(0, 2);
                String mantissa = input.substring(3);
                if (exp.contains("+")) {
                    // Positive Number
                    exp = positiveNumsEncodeToIntExponentsMap.get(exp);
                    output = new BigDecimal(mantissa + "E" + exp);
                } else if (exp.contains("!")) {
                    // Negative Number
                    exp = negativeNumEncodeToIntExponentsMap.get(exp);
                    output = new BigDecimal(mantissa).subtract(BigDecimal.TEN).movePointRight(Integer.parseInt(exp));
                } else {
                    throw new NumberFormatException("Unknown encoded exponent");
                }
                
            } catch (Exception ex) {
                throw new IllegalArgumentException("Error decoding output: " + input + " . Error: " + ex, ex);
            }
        }
        return output;
    }
    
    public static char getPositiveBin(int index) {
        return positiveNumsIntToEncodeExponentsMap.get(String.valueOf(index)).charAt(1);
    }
    
    public static char getNegativeBin(int index) {
        return negativeNumIntToEncodeExponentsMap.get(String.valueOf(index)).charAt(1);
    }
    
    private static void initPositiveExponents() {
        // The order of the encoded characters here maps directly to how their corresponding exponent value is calculated, and must not be changed.
        List<String> exponents = new ArrayList<>();
        uppercaseLetters.stream().map(letter -> "+" + letter).forEach(exponents::add);
        lowercaseLetters.stream().map(letter -> "+" + letter).forEach(exponents::add);
        Map<String,String> map = createExponentMap(exponents);
        positiveNumsEncodeToIntExponentsMap = Collections.unmodifiableMap(map);
        positiveNumsIntToEncodeExponentsMap = Collections.unmodifiableMap(invertMap(map));
    }
    
    private static void initNegativeExponents() {
        // The order of the encoded characters here maps directly to how their corresponding exponent value is calculated, and must not be changed.
        List<String> exponents = new ArrayList<>();
        // Iterate in reverse.
        ListIterator<String> iterator = lowercaseLetters.listIterator(lowercaseLetters.size());
        while (iterator.hasPrevious()) {
            exponents.add("!" + iterator.previous());
        }
        // Iterate in reverse.
        iterator = uppercaseLetters.listIterator(uppercaseLetters.size());
        while (iterator.hasPrevious()) {
            exponents.add("!" + iterator.previous());
        }
        Map<String,String> map = createExponentMap(exponents);
        negativeNumEncodeToIntExponentsMap = Collections.unmodifiableMap(map);
        negativeNumIntToEncodeExponentsMap = Collections.unmodifiableMap(invertMap(map));
    }
    
    private static Map<String,String> createExponentMap(List<String> exponents) {
        Map<String,String> map = new HashMap<>();
        for (int pos = 0; pos < exponents.size(); pos++) {
            int exponent = pos - 26;
            map.put(exponents.get(pos), String.valueOf(exponent));
        }
        return map;
    }
    
    private static Map<String,String> invertMap(Map<String,String> map) {
        return map.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    }
}
