package datawave.data.type.util;

import java.io.Serializable;

/**
 * This normalizer is aiming to remove non-digits from phone numbers.
 */
public class PhoneNumber implements Serializable, Comparable<PhoneNumber> {
    
    private String originalPhoneNumber = "";
    private String normalizedPhoneNumber = "";
    
    /**
     * A valid phone number must contain at least 7 digits.
     */
    private static int MIN_LENGTH = 7;
    
    /**
     * Phone numbers cannot have more than 15 digits.
     */
    private static int MAX_LENGTH = 15;
    
    /**
     * Valid digits.
     */
    private static String DIGITS = "0123456789";
    
    /**
     * @param number
     *            the phone number string
     */
    public PhoneNumber(String number) {
        this(number, number);
    }
    
    /**
     * @param number
     *            the original phone number string
     * @param normalizedNumber
     *            the normalized phone number string
     */
    public PhoneNumber(String number, String normalizedNumber) {
        this.originalPhoneNumber = number;
        this.normalizedPhoneNumber = normalizedNumber;
    }
    
    /**
     * The only normalization this method does is removing spaces and punctuation from the phone number string.
     * 
     * @return the normalized phone number
     */
    public String toNormalizedString() {
        return this.normalizedPhoneNumber;
    }
    
    /**
     * Parse a string and pull out a valid phone number if it exists.
     * 
     * @param number
     *            string to look for a phone number in
     * @return PhoneNumber object for found phone number
     * @throws IllegalArgumentException
     *             if parameter doesn't contain a valid phone number
     */
    public static PhoneNumber parse(String number) {
        String num = basicPhoneNumberCheck(number);
        
        return isValid(num);
    }
    
    /**
     * Perform checks to see if there's a valid phone number.
     * 
     * @param number
     *            the phone number to check. Should be pre-processed to remove leading/trailing words.
     * @return PhoneNumber object for found phone number
     * @throws IllegalArgumentException
     *             if parameter isn't a valid phone number
     */
    private static PhoneNumber isValid(String number) {
        char[] data = number.toCharArray();
        char[] num = new char[data.length];
        int pos = 0;
        int nonDigitCount = 0;
        int plusCount = 0;
        int dashCount = 0;
        int dotCount = 0;
        int spaceCount = 0;
        int openCount = 0;
        int closCount = 0;
        int currentSpanLength = 0;
        int numSingleDigitSpans = 0;
        int numSingleZeroSpans = 0;
        int consecutiveDigit = 0;
        int maxDigitSpan = 0;
        int start = 0;
        int end = data.length - 1;
        
        /**
         * This normalizer is just worrying about stripping punctuation from phone numbers, so if this is a string of digits, just return instead of doing the
         * other checks.
         */
        if (number.matches("^\\d+$")) {
            return new PhoneNumber(number);
        }
        
        for (int i = 0; i < data.length; i++) {
            if (isDigit(data[i])) {
                currentSpanLength++;
                if (pos > 0 && data[i] == num[pos - 1]) {
                    if (consecutiveDigit++ > 4) {
                        throw new IllegalArgumentException("No more than 4 in a row of the same digit is permitted.");
                    }
                }
                
                num[pos++] = data[i];
                
                continue;
            }
            
            if (currentSpanLength > maxDigitSpan) {
                maxDigitSpan = currentSpanLength;
            }
            
            if (currentSpanLength == 1) {
                if (num[pos - 1] == '0') {
                    ++numSingleZeroSpans;
                } else {
                    ++numSingleDigitSpans;
                }
                
                if (numSingleZeroSpans > 1 || numSingleDigitSpans > 1) {
                    throw new IllegalArgumentException("No more than one single digit and one single zero spans are permitted.");
                }
            }
            
            currentSpanLength = 0;
            if (i > start && (!isDigit(data[i - 1]) && !(data[i - 1] == ' ' || data[i] == ' ')
                            && !((data[i - 1] == '+' && data[i] == '(') || (data[i - 1] == '(' && data[i] == '+')))) {
                if (!((data[i - 1] == ')' && data[i] == '-') || (data[i - 1] == '-' && data[i] == '('))) {
                    throw new IllegalArgumentException("No more than one consecutive punctuation charachter is permitted except for '+(' or '(+'");
                }
            }
            
            if (data[i] == ' ') {
                if (i > 3 && data[i - 1] == '-' && data[i - 2] == ' ') {
                    --spaceCount;
                    nonDigitCount -= 2;
                } else {
                    spaceCount++;
                    if (spaceCount > 5 || (spaceCount > 4 && pos < 11 && num[0] != '0')) {
                        throw new IllegalArgumentException("Too many spaces found");
                    }
                }
            }
            
            if (i > 0 && data[i] == ' ' && data[i - 1] == ' ') {
                throw new IllegalArgumentException("No more than one consecutive space is permitted.");
            }
            
            if (data[i] == '(') {
                openCount++;
            } else if (data[i] == ')') {
                closCount++;
            } else if (data[i] == '+' && ++plusCount > 1) {
                throw new IllegalArgumentException("Only one plus sign is allowed.");
            } else if (data[i] == '-' && (++dashCount > 3 || i == start)) {
                throw new IllegalArgumentException("Only three dashes are allowed, and leading dashes are prohibited.");
            } else if (data[i] == '.' && ++dotCount > 2) {
                throw new IllegalArgumentException("Only two dots are allowed.");
            }
            
            if (++nonDigitCount > 7) {
                throw new IllegalArgumentException("Only seven non-digit characters are allowed.");
            }
        }
        
        String s = new String(num, 0, pos);
        
        if (dotCount > 0 && dashCount > 0) {
            throw new IllegalArgumentException("Only one of dots or dashes can be used.");
        } else if (pos == MAX_LENGTH && num[0] != '0') {
            throw new IllegalArgumentException("With max length number there must be a leading zero");
        }
        
        int countLeadingZeroOrOne = 0;
        int ix = 0;
        while (ix < pos && (num[ix] == '0' || num[ix] == '1')) {
            ix++;
        }
        
        if (pos < MIN_LENGTH + ix) {
            throw new IllegalArgumentException("Ignoring leading zeroes and ones, the number is not long enough");
        } else if (ix + 3 < pos && num[ix] == num[ix + 1] && num[ix] == num[ix + 2] && num[ix] == num[ix + 3] && num[ix] != 8) {
            throw new IllegalArgumentException(
                            "No more than three consecutive same digits after the leading ones and zeroes are permitted unless the digit is '8'.");
        } else if (dotCount == 1 && (pos > 7 || pos == 7 && data[start + 3] != '.')) {
            throw new IllegalArgumentException("If the number contains only one dot, it must contain 7 digits in the form XXX.XXXX");
        } else if (openCount + closCount > 0 && openCount != closCount) {
            throw new IllegalArgumentException("Parenthesis mis-match");
        } else if (dotCount + dashCount + plusCount + openCount + closCount == 0 && (pos < 8 || (pos > 11 && spaceCount < 3 && currentSpanLength < 5))) {
            throw new IllegalArgumentException("Number is the wrong length to have no puctuation but spaces.");
        } else if (pos < 8 && (num[0] == '1' || num[0] == '0')) {
            throw new IllegalArgumentException("Number is too short to have a leading one or zero");
        } else if (num[0] == '0' && num[1] == '0' && num[2] == '0') {
            throw new IllegalArgumentException("Too many leading zeroes.");
        } else if (currentSpanLength < 3 && pos < 10) {
            throw new IllegalArgumentException("Valid numbers must be longer to end in a digit span of one or two.");
        }
        
        if (data[start] != '+' && isISBN(s) && (spaceCount > 0 || dashCount > 0 || dotCount > 0) && (openCount + closCount) == 0) {
            throw new IllegalArgumentException("Looks like an ISBN");
        } else if (number.matches("^\\d\\d\\d([ \\-])\\d\\d\\1\\d\\d\\d\\d$")) {
            throw new IllegalArgumentException(number + " looks like a SSN");
        } else if (number.matches("^[12]\\d\\d\\d ?- ?[12]\\d\\d\\d$")) {
            throw new IllegalArgumentException(number + " looks like a year range");
        } else if (number.matches("^(19|20)\\d\\d([\\-\\. ])[01]\\d\\2[0-3]\\d$")) {
            throw new IllegalArgumentException(number + " looks like a yyyy mm dd date");
        } else if (number.matches("^(19|20)\\d\\d[01]\\d[0-3]\\d$")) {
            throw new IllegalArgumentException(number + " looks like a yyyymmdd date");
        } else if (number.matches("^(19|20)\\d\\d([\\-\\. ])?[0-3]\\d\\2[01]\\d$")) {
            throw new IllegalArgumentException(number + " looks like a yyyy dd mm date");
        } else if (number.matches("^(19|20)\\d\\d[0-3]\\d[01]\\d$")) {
            throw new IllegalArgumentException(number + " looks like a yyyyddmm date");
        } else if (number.matches("^[0-3]\\d([\\-\\.])[01]\\d\\1(19|20)\\d\\d ([0-1]\\d|2[0-4])$")) {
            throw new IllegalArgumentException(number + " looks like a dd-mm-yyyy hh:mm date");
        } else if (number.matches("^[0-3]\\d([\\-\\.])[1-9]\\1(19|20)\\d\\d ([0-1]\\d|2[0-4])$")) {
            throw new IllegalArgumentException(number + " looks like a dd-mm-yyyy hh:mm date");
        } else if (number.matches("^(19|20)\\d\\d([\\-\\. ])([0-2]\\d\\d|3[0-5]\\d|36[0-6])$")) {
            throw new IllegalArgumentException(number + " looks like a yyyy jjj date");
        }
        
        return new PhoneNumber(number, s);
    }
    
    /**
     * This will go through the data string looking for a phone number.
     * 
     * @param number
     *            The data to look for phone numbers in
     * @return A string containing what is believed to be a phone number
     * @throws IllegalArgumentException
     *             If data does not contain a possible phone number
     */
    private static String basicPhoneNumberCheck(String number) {
        /**
         * This normalizer is just worrying about stripping punctuation from phone numbers, so if this is a string of digits, just return instead of doing the
         * other checks.
         */
        if (number.matches("^\\d+$")) {
            return number;
        }
        
        char[] data = number.toCharArray();
        
        if (data == null) {
            throw new IllegalArgumentException("The character array of the string argument is null");
        } else if (data.length < MIN_LENGTH) {
            throw new IllegalArgumentException("The data must be at least " + MIN_LENGTH + " characters long.  Found " + data.length + " characters.");
        }
        
        // trim down the string to pick out phone numbers
        for (int i = MIN_LENGTH; i < data.length; i++) {
            if (!isDigit(data[i])) {
                continue;
            }
            int start = i - 1;
            
            while (start >= 0 && isPhoneNumberCharacter(data[start]) && (i - start) <= MAX_LENGTH) {
                if ((!isDigit(data[start])) && data[start] == data[start + 1]) {
                    break;
                }
                start--;
            }
            
            if (start == -1 || !isPhoneNumberCharacter(data[start])) {
                start++;
            }
            
            int seqlen = countDigits(data, start, i);
            if (seqlen < MIN_LENGTH || seqlen > MAX_LENGTH) {
                continue;
            }
            
            if (start > 1 && data[start - 1] == ':' && isDigit(data[start - 2])) {
                boolean spaceok = false;
                for (int j = start; j < i; j++) {
                    spaceok = i - j >= MIN_LENGTH;
                    if (spaceok) {
                        start = j + 1;
                    }
                    break;
                }
                if (!spaceok) {
                    continue;
                }
            }
            
            while (data[start] == ')' || data[start] == ' ' || data[start] == '.' || data[start] == '-') {
                start++;
            }
            
            while (i + 1 < data.length && isPhoneNumberCharacter(data[i + 1])) {
                i++;
            }
            
            while (data[i] == ' ') {
                i--;
            }
            
            int lastSpace = i;
            while (lastSpace > start && data[lastSpace] != ' ') {
                lastSpace--;
            }
            
            if (lastSpace < i && lastSpace > start) {
                while (!isDigit(data[i]) && i >= lastSpace) {
                    i--;
                }
                
                while (data[i] == ' ') {
                    i--;
                }
            }
            
            String rawString = new String(data, start, i - start + 1);
            if (start > 0 && Character.isLetter(data[start - 1])) {
                continue;
            } else if (i < data.length - 2 && data[i + 1] == ',' && isDigit(data[i + 2])) {
                continue;
            } else if (i < data.length - 3 && data[i + 1] == ']' && data[i + 2] == ')' && start > 2 && data[start - 1] == '[' && data[start - 2] == '(') {
                continue;
            } else if (countDigits(data, start, i) > MAX_LENGTH) {
                continue;
            }
            
            if (data[start] == '+' && data[start + 1] == '+') {
                start++;
            }
            
            if (data[i] == '.') {
                i--;
            }
            
            if (i < data.length - 1 && ((Character.isLetter(data[i + 1]) && data[i + 1] != 'x' && data[i] != 'X') || data[i + 1] == '_' || data[i + 1] == '='
                            || data[i + 1] == '?' || data[i + 1] == '\\')) {
                continue;
            } else if (i < data.length - 2 && data[i + 1] == '/' && !isDigit(data[i + 2])) {
                continue;
            } else if (i < data.length - 2 && data[i + 1] == '.' && !Character.isWhitespace(data[i + 2])) {
                continue;
            } else if (start > 0 && (data[start - 1] == '=' || data[start - 1] == '*' || data[start - 1] == '/' || data[start - 1] == '_'
                            || data[start - 1] == '?' || data[start - 1] == ',' || data[start - 1] == '$'
                            || (i + 1 < data.length && data[start - 1] == '.' && data[i + 1] == '.') || (data[start - 1] == '.' && data[i] == '.'))) {
                continue;
            }
            
            return new String(data, start, i - start + 1);
        }
        throw new IllegalArgumentException("Did not find a phone number!");
    }
    
    @Override
    public String toString() {
        return this.originalPhoneNumber;
    }
    
    @Override
    public int compareTo(PhoneNumber o) {
        return this.toNormalizedString().compareTo(o.toNormalizedString());
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof PhoneNumber) {
            /**
             * Consider phone numbers equal if they have the same normalized form.
             */
            return this.toNormalizedString().equals(((PhoneNumber) o).toNormalizedString());
        } else {
            return false;
        }
    }
    
    @Override
    public int hashCode() {
        return this.toNormalizedString().hashCode();
    }
    
    /**
     * Test if character is a digit.
     * 
     * @param d
     *            the character to test
     * @return whether or not the character is a digit
     */
    private static boolean isDigit(char d) {
        if (DIGITS.indexOf(d) >= 0) {
            return true;
        }
        return false;
    }
    
    /**
     * Tests if character is a phone number character (digits, spaces, parens, dash, plus, dot).
     * 
     * @param c
     *            the character to test
     * @return whether or not the character is a phone number character
     */
    private static boolean isPhoneNumberCharacter(char c) {
        return ((isDigit(c) || c == ' ' || c == '(' || c == ')' || c == '-' || c == '+' || c == '.'));
    }
    
    /**
     * Count the number of digits in a character array.
     * 
     * @param data
     *            The character array to count digits in
     * @param start
     *            The start offset for the character array
     * @param end
     *            The end offset for the character array
     * @return The number of digits in the character array in positions [start, end]
     */
    private static int countDigits(char data[], int start, int end) {
        int count = 0;
        char c;
        for (int i = start; i <= end; i++) {
            c = data[i];
            if (isDigit(c)) {
                count++;
            }
        }
        return count;
    }
    
    /**
     * Test if string is an ISBN.
     * 
     * @param s
     *            The string to test
     * @return whether or not the string is an ISBN
     */
    private static boolean isISBN(String s) {
        if (s.length() == 10) {
            int sum = 0;
            for (int i = 0; i < s.length(); i++) {
                if (isDigit(s.charAt(i))) {
                    sum += (Character.digit(s.charAt(i), 10) * (10 - i));
                } else if (s.charAt(i) == 'X' && i == 9) {
                    sum += 10;
                }
            }
            return sum % 11 == 0;
        } else if (s.length() == 13 && (s.startsWith("978") || s.startsWith("979"))) {
            int sum = 0;
            for (int i = 0; i < s.length(); i++) {
                sum += (Character.digit(s.charAt(i), 10) * (i % 2 == 0 ? 1 : 3));
            }
            return sum % 10 == 0;
        }
        return false;
    }
}
