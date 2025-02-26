package datawave.data.type.util;

import java.io.Serializable;
import java.util.Objects;

/**
 * Class to hold a MAC Address
 */
public class MACAddress implements Serializable, Comparable<MACAddress> {
    
    private static final long serialVersionUID = 4366259028581959024L;
    
    /**
     * String representation of the MAC address
     */
    private String macAddress = "";
    
    /**
     * The separator used between digit groups.
     */
    private String separator = "";
    
    /**
     * The size of the digit groups.
     */
    private int groupingSize = 0;
    
    /**
     * MAC addresses contain 12 digits
     */
    private static final int MAC_ADDRESS_LENGTH = 12;
    
    /**
     * The number of groupings
     */
    private int groupings = 0;
    
    /**
     * @param addr
     *            string representation of the MAC address
     * @param sep
     *            separator used in the MAC address
     * @param groupingSize
     *            size of the digit groups
     */
    public MACAddress(String addr, String sep, int groupingSize) {
        this.macAddress = addr;
        this.separator = sep;
        this.groupingSize = groupingSize;
        this.groupings = MAC_ADDRESS_LENGTH / this.groupingSize;
    }
    
    /**
     * Normalize the string representation of the MAC address. Defaults to using a grouping size of 2
     * 
     * @param sep
     *            The separator to use in the normalized string.
     * @return The normalized string
     */
    public String toNormalizedString(String sep) {
        return toNormalizedString(sep, 2);
    }
    
    /**
     * Normalize the string representation of the MAC address.
     * 
     * @param sep
     *            The separator to use in the normalized string
     * @param groupingSize
     *            The grouping size to use in the normalized string
     * @return the normalized string
     */
    public String toNormalizedString(String sep, int groupingSize) {
        String returnAddress = new String(this.macAddress);
        
        if (!this.separator.equals("")) {
            String sepRegex = new String(this.separator);
            if (this.separator.matches("\\.")) {
                sepRegex = "\\" + sepRegex;
            }
            returnAddress = returnAddress.replaceAll(this.separator, "");
        }
        
        String hexDigit = "([0-9a-fA-F])";
        StringBuilder hexDigits = new StringBuilder();
        // populate hexDigits as a regex to capture 12 hex digits
        for (int i = 0; i < MAC_ADDRESS_LENGTH; i++) {
            hexDigits.append(hexDigit);
        }
        
        StringBuilder replacement = new StringBuilder();
        int groups = MAC_ADDRESS_LENGTH / groupingSize;
        int totalStringLength = MAC_ADDRESS_LENGTH + groups - 1;
        int digitCount = 1;
        String sepRegex = new String(sep);
        if (sepRegex.matches("\\.")) {
            sepRegex = "\\" + sepRegex;
        }
        // populate replacement as a regex to properly format / separate the hex digits
        for (int i = 1; i <= totalStringLength; i++) {
            if (i % (groupingSize + 1) == 0) {
                replacement.append(sepRegex);
            } else {
                replacement.append("$" + digitCount);
                digitCount++;
            }
        }
        
        returnAddress = returnAddress.replaceAll(hexDigits.toString(), replacement.toString());
        returnAddress = returnAddress.toUpperCase();
        
        return returnAddress;
    }
    
    /**
     * Attempt to parse a MAC address
     * 
     * @param addr
     *            The MAC address
     * @param sep
     *            The string separating hex digits
     * @param groupingSize
     *            The size of the hex digit groups
     * @param strict
     *            If true, will do extra checks to make sure it looks like a MAC address
     * @return the MACAddress object
     * @throws IllegalArgumentException
     *             if unable to parse out a MAC address
     */
    public static MACAddress parse(String addr, String sep, int groupingSize, boolean strict) {
        if (addr.contains(sep)) {
            if (groupingSize < 1 || groupingSize > MAC_ADDRESS_LENGTH) {
                throw new IllegalArgumentException("Grouping size must be between 1 and " + MAC_ADDRESS_LENGTH + ", inclusive.");
            }
            if (sep.matches("\\.")) {
                sep = "\\" + sep;
            }
            String[] digits;
            if (!sep.equals("")) {
                digits = addr.split(sep);
            } else {
                digits = new String[1];
                digits[0] = addr;
            }
            int numberOfGroupings = MAC_ADDRESS_LENGTH / groupingSize;
            if (digits.length != numberOfGroupings) {
                throw new IllegalArgumentException("Address " + addr + " is not " + numberOfGroupings + " groups of digits divided by " + sep);
            }
            for (String digit : digits) {
                if (digit.length() != groupingSize) {
                    throw new IllegalArgumentException("Digit block " + digit + " is not " + groupingSize + " digits.");
                }
                Long.parseLong(digit, 16);
            }
            // If this doesn't look like a standard MAC address, make sure it has hex digits to avoid picking up
            // IPs, etc.
            if (strict && ((groupingSize != 2 && groupingSize != 4) || (!sep.equals(".") && !sep.equals(":") && !sep.equals("-")))) {
                String addrNoSep = new String(addr);
                if (!sep.equals("")) {
                    addrNoSep = addrNoSep.replaceAll(sep, "");
                }
                try {
                    Long.parseLong(addrNoSep, 10);
                    throw new IllegalArgumentException("This has no hex strings, probably not a mac address");
                } catch (NumberFormatException e) {
                    // This is OK, means it has hex digits
                }
            }
            return new MACAddress(addr, sep, groupingSize);
        } else {
            throw new IllegalArgumentException("Address " + addr + " does not contain separator " + sep);
        }
    }
    
    /**
     * Attempt to parse a MAC address The separator will be guessed based on the grouping size
     * 
     * @param addr
     *            The MAC address
     * @param groupingSize
     *            The size of the hex digit group
     * @return the MAC address object
     * @throws IllegalArgumentException
     *             if unable to parse a MAC address
     */
    public static MACAddress parse(String addr, int groupingSize) {
        if (groupingSize < 1 || groupingSize > MAC_ADDRESS_LENGTH) {
            throw new IllegalArgumentException("Grouping size must be between 1 and " + MAC_ADDRESS_LENGTH + ", inclusive");
        }
        String sep = "";
        if (groupingSize != MAC_ADDRESS_LENGTH) {
            sep = String.valueOf(addr.charAt(groupingSize));
        }
        
        return parse(addr, sep, groupingSize, true);
    }
    
    /**
     * Attempt to parse a MAC address The grouping size will be guessed based on the separator
     * 
     * @param addr
     *            the MAC address
     * @param sep
     *            the separator
     * @return the MAC address object
     * @throws IllegalArgumentException
     *             if unable to parse a MAC address
     */
    public static MACAddress parse(String addr, String sep) {
        if (!addr.contains(sep)) {
            throw new IllegalArgumentException("Separator " + sep + " not found in " + addr);
        }
        int groupingSize = addr.indexOf(sep);
        return parse(addr, sep, groupingSize, true);
    }
    
    /**
     * Attempt to parse a MAC address The grouping size and separator will be guessed
     * 
     * @param addr
     *            the MAC address
     * @return the MAC address object
     * @throws IllegalArgumentException
     *             if unable to parse a MAC address
     */
    public static MACAddress parse(String addr) {
        if (addr.matches("^[0-9a-fA-F]+$")) {
            return parse(addr, "", MAC_ADDRESS_LENGTH, true);
        } else if (addr.matches("^([0-9a-fA-F]+[^0-9a-fA-F])+[0-9a-fA-F]+$")) {
            String[] pieces = addr.split("[^0-9a-fA-F]");
            int groupingSize = MAC_ADDRESS_LENGTH / pieces.length;
            String sep = String.valueOf(addr.charAt(groupingSize));
            return parse(addr, sep, groupingSize, true);
        } else {
            throw new IllegalArgumentException("Unable to find separator in " + addr);
        }
    }
    
    @Override
    public String toString() {
        return this.macAddress;
    }
    
    @Override
    public int compareTo(MACAddress o) {
        return this.toString().compareTo(o.toString());
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof MACAddress) {
            /**
             * Consider the MAC addresses equal if they have the same normalized string
             */
            return this.toNormalizedString("").equals(((MACAddress) o).toNormalizedString(""));
        } else {
            return false;
        }
    }
    
    @Override
    public int hashCode() {
        return this.toNormalizedString("").hashCode();
    }
}
