package datawave.data.type.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides a one-to-one mapping between an input decimal number and a lexigraphically sorted index for that number. The index is composed of two parts, roughly
 * derived from scientific notation: the two digit exponential bin and the mantissa, with 'E' as a seperator. Thus an index takes this format: 'bin'E'mantissa'.
 * 
 * The bins are broken into four groups: !A through !Z represent negative numbers with magnitude greater than one (exponents 25 through 0, respectively) !a
 * through !z represent negative numbers with magnitude less than 1 (exponents -1 through -26, respectively) +A through +Z represent positivee numbers with
 * magnitude less than 1 (exponents -26 through -1, respectively) +a through +z represent positive numbers with magnitude greater than one (exponents 0 through
 * 25, respectively)
 * 
 * For positive numbers, the mantissa exactly matches the mantissa of scientific notation. For negative numbers, the mantissa equals ten minus the mantissa of
 * scientific notation.
 * 
 * Some example inputs and encodings: -12344984165 !PE8.7655015835 -500 !XE5 -0.501 !aE4.99 0 +AE0 9E-9 +RE9 0.501 +ZE5.01 10000 +eE1
 * 
 */
public class NumericalEncoder {
    
    private static Map<String,String> positiveNumsEncodeToIntExponentsMap;
    private static Map<String,String> positiveNumsIntToEncodeExponentsMap;
    private static Map<String,String> negativeNumEncodeToIntExponentsMap;
    private static Map<String,String> negativeNumIntToEncodeExponentsMap;
    private static NumberFormat plainFormatter = new DecimalFormat("0.#########################################################");
    private static NumberFormat scientificFormatter = new DecimalFormat("0.#########################################################E0");
    private static final String zero = "+AE0";
    
    static {
        initNegativeExponents();
        initPositiveExponents();
    }
    
    public static String encode(String input) {
        try {
            BigDecimal decimal = new BigDecimal(input);
            String encodedExponent = "";
            String mantissa = "";
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
                throw new NumberFormatException("Exponenent exceeded allowed range.");
            }
            
            return encodedExponent + "E" + mantissa;
        } catch (Exception ex) {
            throw new NumberFormatException("Error formatting input: " + input + " . Error: " + ex.toString());
        }
    }
    
    /**
     * This provides a quick test that will determine whether this value is possibly encoded. Provides a mechanism that is significantly faster than waiting for
     * the decode method to throw an exception.
     * 
     * @param input
     * @return true if possibly encoded, false if definitely no encoded
     */
    public static boolean isPossiblyEncoded(String input) {
        if (null == input || input.isEmpty())
            return false;
        char c = input.charAt(0);
        return (c == '+' || c == '!');
    }
    
    public static BigDecimal decode(String input) {
        BigDecimal output = new BigDecimal(BigInteger.ONE);
        if (input.equals(zero)) {
            return BigDecimal.ZERO;
        } else {
            try {
                String exp = input.substring(0, 2);
                String mantissa = input.substring(3, input.length());
                if (exp.contains("+")) {
                    // Positive Number
                    exp = positiveNumsEncodeToIntExponentsMap.get(exp);
                    output = new BigDecimal(mantissa + "E" + exp);
                } else if (exp.contains("!")) {
                    // Negative Number
                    exp = negativeNumEncodeToIntExponentsMap.get(exp);
                    output = new BigDecimal(mantissa).subtract(BigDecimal.TEN).movePointRight(Integer.valueOf(exp));
                } else {
                    throw new NumberFormatException("Unknown encoded exponent");
                }
                
            } catch (Exception ex) {
                throw new NumberFormatException("Error decoding output: " + input + " . Error: " + ex.toString());
            }
        }
        return output;
    }
    
    static void initPositiveExponents() {
        String[] positiveExponents;
        positiveExponents = new String[52];
        positiveExponents[0] = "+A";
        positiveExponents[1] = "+B";
        positiveExponents[2] = "+C";
        positiveExponents[3] = "+D";
        positiveExponents[4] = "+E";
        positiveExponents[5] = "+F";
        positiveExponents[6] = "+G";
        positiveExponents[7] = "+H";
        positiveExponents[8] = "+I";
        positiveExponents[9] = "+J";
        positiveExponents[10] = "+K";
        positiveExponents[11] = "+L";
        positiveExponents[12] = "+M";
        positiveExponents[13] = "+N";
        positiveExponents[14] = "+O";
        positiveExponents[15] = "+P";
        positiveExponents[16] = "+Q";
        positiveExponents[17] = "+R";
        positiveExponents[18] = "+S";
        positiveExponents[19] = "+T";
        positiveExponents[20] = "+U";
        positiveExponents[21] = "+V";
        positiveExponents[22] = "+W";
        positiveExponents[23] = "+X";
        positiveExponents[24] = "+Y";
        positiveExponents[25] = "+Z";
        positiveExponents[26] = "+a";
        positiveExponents[27] = "+b";
        positiveExponents[28] = "+c";
        positiveExponents[29] = "+d";
        positiveExponents[30] = "+e";
        positiveExponents[31] = "+f";
        positiveExponents[32] = "+g";
        positiveExponents[33] = "+h";
        positiveExponents[34] = "+i";
        positiveExponents[35] = "+j";
        positiveExponents[36] = "+k";
        positiveExponents[37] = "+l";
        positiveExponents[38] = "+m";
        positiveExponents[39] = "+n";
        positiveExponents[40] = "+o";
        positiveExponents[41] = "+p";
        positiveExponents[42] = "+q";
        positiveExponents[43] = "+r";
        positiveExponents[44] = "+s";
        positiveExponents[45] = "+t";
        positiveExponents[46] = "+u";
        positiveExponents[47] = "+v";
        positiveExponents[48] = "+w";
        positiveExponents[49] = "+x";
        positiveExponents[50] = "+y";
        positiveExponents[51] = "+z";
        
        positiveNumsEncodeToIntExponentsMap = new HashMap<String,String>();
        positiveNumsIntToEncodeExponentsMap = new HashMap<String,String>();
        for (int j = 0; j < positiveExponents.length; j++) {
            int exponent = j - 26;
            positiveNumsEncodeToIntExponentsMap.put(positiveExponents[j], String.valueOf(exponent));
            positiveNumsIntToEncodeExponentsMap.put(String.valueOf(exponent), positiveExponents[j]);
        }
    }
    
    private static void initNegativeExponents() {
        String[] negativeExponents;
        negativeExponents = new String[52];
        negativeExponents[51] = "!A";
        negativeExponents[50] = "!B";
        negativeExponents[49] = "!C";
        negativeExponents[48] = "!D";
        negativeExponents[47] = "!E";
        negativeExponents[46] = "!F";
        negativeExponents[45] = "!G";
        negativeExponents[44] = "!H";
        negativeExponents[43] = "!I";
        negativeExponents[42] = "!J";
        negativeExponents[41] = "!K";
        negativeExponents[40] = "!L";
        negativeExponents[39] = "!M";
        negativeExponents[38] = "!N";
        negativeExponents[37] = "!O";
        negativeExponents[36] = "!P";
        negativeExponents[35] = "!Q";
        negativeExponents[34] = "!R";
        negativeExponents[33] = "!S";
        negativeExponents[32] = "!T";
        negativeExponents[31] = "!U";
        negativeExponents[30] = "!V";
        negativeExponents[29] = "!W";
        negativeExponents[28] = "!X";
        negativeExponents[27] = "!Y";
        negativeExponents[26] = "!Z";
        negativeExponents[25] = "!a";
        negativeExponents[24] = "!b";
        negativeExponents[23] = "!c";
        negativeExponents[22] = "!d";
        negativeExponents[21] = "!e";
        negativeExponents[20] = "!f";
        negativeExponents[19] = "!g";
        negativeExponents[18] = "!h";
        negativeExponents[17] = "!i";
        negativeExponents[16] = "!j";
        negativeExponents[15] = "!k";
        negativeExponents[14] = "!l";
        negativeExponents[13] = "!m";
        negativeExponents[12] = "!n";
        negativeExponents[11] = "!o";
        negativeExponents[10] = "!p";
        negativeExponents[9] = "!q";
        negativeExponents[8] = "!r";
        negativeExponents[7] = "!s";
        negativeExponents[6] = "!t";
        negativeExponents[5] = "!u";
        negativeExponents[4] = "!v";
        negativeExponents[3] = "!w";
        negativeExponents[2] = "!x";
        negativeExponents[1] = "!y";
        negativeExponents[0] = "!z";
        
        negativeNumEncodeToIntExponentsMap = new HashMap<String,String>();
        negativeNumIntToEncodeExponentsMap = new HashMap<String,String>();
        for (int j = 0; j < negativeExponents.length; j++) {
            int exponent = j - 26;
            negativeNumEncodeToIntExponentsMap.put(negativeExponents[j], String.valueOf(exponent));
            negativeNumIntToEncodeExponentsMap.put(String.valueOf(exponent), negativeExponents[j]);
        }
    }
}
