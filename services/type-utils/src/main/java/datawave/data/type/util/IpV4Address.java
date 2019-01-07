package datawave.data.type.util;

import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

/**
 * The IpV4 address
 * 
 */
public class IpV4Address extends IpAddress {
    private static final long serialVersionUID = -3258500702340145500L;
    private byte[] ipaddress = new byte[4];
    private int wildcardLoc = -1;
    private int numOctets = 4;
    
    public IpV4Address(byte[] address) {
        if (address.length != 4) {
            throw new IllegalArgumentException("An IPV4 address must be 4 bytes in length");
        }
        System.arraycopy(address, 0, this.ipaddress, 0, 4);
    }
    
    public IpV4Address(byte[] address, int wildcardLoc, int numOctets) {
        this.wildcardLoc = wildcardLoc;
        this.numOctets = numOctets;
        if (address.length != 4) {
            throw new IllegalArgumentException("An IPV4 address must be 4 bytes in length");
        }
        System.arraycopy(address, 0, this.ipaddress, 0, 4);
    }
    
    public IpV4Address(long ipaddress) {
        if ((ipaddress >>> 32) != 0) {
            throw new IllegalArgumentException(ipaddress + " is out of range");
        }
        this.ipaddress[3] = (byte) (0x00FF & (ipaddress >>> 0));
        this.ipaddress[2] = (byte) (0x00FF & (ipaddress >>> 8));
        this.ipaddress[1] = (byte) (0x00FF & (ipaddress >>> 16));
        this.ipaddress[0] = (byte) (0x00FF & (ipaddress >>> 24));
    }
    
    /**
     * Return the underlying bytes
     * 
     * @return the IpV4 address bytes
     */
    public byte[] toBytes() {
        return new byte[] {this.ipaddress[0], this.ipaddress[1], this.ipaddress[2], this.ipaddress[3]};
    }
    
    /**
     * Return the underlying bytes in reverse order
     * 
     * @return the IpV4 address bytes in reverse order
     */
    public byte[] toReverseBytes() {
        return new byte[] {this.ipaddress[3], this.ipaddress[2], this.ipaddress[1], this.ipaddress[0]};
    }
    
    /**
     * Return the int representation of this address
     * 
     * @return an int
     */
    public long toNumber() {
        long value = 0x00FF & ipaddress[0];
        value <<= 8;
        value |= 0x00FF & ipaddress[1];
        value <<= 8;
        value |= 0x00FF & ipaddress[2];
        value <<= 8;
        value |= 0x00FF & ipaddress[3];
        return value;
    }
    
    /**
     * Return the int representation of this address
     * 
     * @return an int
     */
    public long toReverseNumber() {
        long value = 0x00FF & ipaddress[3];
        value <<= 8;
        value |= 0x00FF & ipaddress[2];
        value <<= 8;
        value |= 0x00FF & ipaddress[1];
        value <<= 8;
        value |= 0x00FF & ipaddress[0];
        return value;
    }
    
    /**
     * Parse an address assume the specified radix
     * 
     * @param address
     * @param radix
     *            The radix (e.g. 10 for decimal, 16 for hexidecimal, ...). 0 means that Number.decode() will be used
     * @param dotted
     *            true if a dot notation, false if simply a number
     * @return the IpV4 address
     * @throws IllegalArgumentException
     *             if the radix is not 0, 10, 8, 16, or the address cannot be parsed
     * @throws NumberFormatException
     *             if a number cannot be parsed using the specified radix
     */
    public static IpV4Address parse(String address, int radix, boolean dotted) {
        if (radix != 0 && radix != 10 && radix != 16 && radix != 8) {
            throw new IllegalArgumentException("Radix " + radix + " is not 0, 8, 10, or 16");
        }
        if (dotted) {
            int wildcard = address.indexOf('*');
            String[] parts = Iterables.toArray(Splitter.on('.').split(address), String.class);
            if (parts.length != 4 && wildcard == -1) {
                throw new IllegalArgumentException("Expected 4 parts but got " + parts.length + " for " + address);
            } else if (wildcard > -1) {
                // if 1.1.* need to make it 001.001.000.000 and mark the wildcard location
                byte[] ipaddress = new byte[4];
                int wc_octet = 0;
                // work backwards
                for (int i = 3; i >= 0; i--) {
                    if (i >= parts.length) {
                        // we need to pad
                        ipaddress[i] = (byte) 0;
                        wc_octet = i;
                    } else if (parts[i].isEmpty() || parts[i].equals("*")) {
                        // pad remainder with zeros and mark location
                        ipaddress[i] = (byte) 0;
                        wc_octet = i;
                    } else {
                        int value = 0;
                        if (!parts[i].isEmpty()) {
                            value = (radix == 0 ? Integer.decode(parts[i]) : Integer.parseInt(parts[i], radix));
                        }
                        if ((value >>> 8) != 0) {
                            throw new IllegalArgumentException("Part " + parts[i] + " of " + address + " is out of range in radix " + radix);
                        }
                        ipaddress[i] = (byte) value;
                    }
                }
                return new IpV4Address(ipaddress, wc_octet, parts.length);
            } else {
                byte[] ipaddress = new byte[4];
                for (int i = 0; i < 4; i++) {
                    if ((radix == 0 && parts[i].length() > 4) || (radix == 10 && parts[i].length() > 3) || (radix == 16 && parts[i].length() > 2)
                                    || (radix == 8 && parts[i].length() > 4)) {
                        throw new IllegalArgumentException("Part " + parts[i] + " of " + address + " is has too many digits for radix " + radix);
                    }
                    int value = 0;
                    if (!parts[i].isEmpty()) {
                        value = (radix == 0 ? Integer.decode(parts[i]) : Integer.parseInt(parts[i], radix));
                    }
                    if ((value >>> 8) != 0) {
                        throw new IllegalArgumentException("Part " + parts[i] + " of " + address + " is out of range in radix " + radix);
                    }
                    ipaddress[i] = (byte) value;
                }
                return new IpV4Address(ipaddress);
            }
        } else {
            long ipaddress = (radix == 0 ? Long.decode(address) : Long.parseLong(address, radix));
            if ((ipaddress >>> 32) != 0) {
                throw new IllegalArgumentException(address + " is out of range in radix " + radix);
            }
            return new IpV4Address(ipaddress);
        }
    }
    
    /**
     * Parse an address assume the specified radix. It attempts first as a dotted notation, then as a single number
     * 
     * @param address
     * @param radix
     *            10 for decimal, 8 for octal, 16 for hexidecimal, 0 to use Number.decode
     * @return An IpV4Address
     * @throws IllegalArgumentException
     *             if the radix is not 0, 10, 8, 16, or the address cannot be parsed
     * @throws NumberFormatException
     *             if a number cannot be parsed using the specified radix
     */
    public static IpV4Address parse(String address, int radix) {
        try {
            return IpV4Address.parse(address, radix, true);
        } catch (Exception iae) {
            return IpV4Address.parse(address, radix, false);
        }
    }
    
    /**
     * Parse an address. It attempts first as radix 10, then as radix 16, then as radix 8, then as radix 0
     * 
     * @param address
     * @return An IpV4Address
     * @throws IllegalArgumentException
     *             if it cannot be parsed
     */
    public static IpV4Address parse(String address, boolean dotted) {
        try {
            return IpV4Address.parse(address, 10, dotted);
        } catch (Exception iae10) {
            try {
                return IpV4Address.parse(address, 16, dotted);
            } catch (Exception iae16) {
                try {
                    return IpV4Address.parse(address, 8, dotted);
                } catch (Exception iae8) {
                    return IpV4Address.parse(address, 0, dotted);
                }
            }
        }
    }
    
    /**
     * Parse an address. It attempts first as radix 10, then as radix 16, then as radix 8, then as radix 0
     * 
     * @param address
     * @return An IpV4Address
     * @throws IllegalArgumentException
     *             if it cannot be parsed
     */
    public static IpV4Address parse(String address) {
        try {
            return IpV4Address.parse(address, 10);
        } catch (Exception iae10) {
            try {
                return IpV4Address.parse(address, 16);
            } catch (Exception iae16) {
                try {
                    return IpV4Address.parse(address, 8);
                } catch (Exception iae8) {
                    return IpV4Address.parse(address, 0);
                }
            }
        }
    }
    
    public static String toString(byte[] address, boolean zeroPadded, int wc_loc, int numOctets, boolean reverse) {
        StringBuilder builder = new StringBuilder(15);
        for (int i = 0; i < address.length; i++) {
            if (wc_loc != -1 && numOctets - 1 < i) {
                break;
            }
            
            if (builder.length() > 0) {
                builder.append('.');
            }
            
            if (i == wc_loc) {
                builder.append("*");
                if (wc_loc != 0) {
                    break;
                }
            } else {
                String value = Integer.toString(0x00FF & address[i]);
                if (zeroPadded) {
                    for (int j = value.length(); j < 3; j++) {
                        builder.append('0');
                    }
                }
                builder.append(value);
            }
            
        }
        return builder.toString();
    }
    
    @Override
    public String toString() {
        return toString(ipaddress, false, this.wildcardLoc, this.numOctets, false);
    }
    
    @Override
    public String toZeroPaddedString() {
        return toString(ipaddress, true, this.wildcardLoc, this.numOctets, false);
    }
    
    @Override
    public String toReverseString() {
        if (wildcardLoc > -1) {
            return toString(toReverseBytes(), false, 3 - this.wildcardLoc, this.numOctets, true);
        } else {
            return toString(toReverseBytes(), false, this.wildcardLoc, this.numOctets, true);
        }
    }
    
    @Override
    public String toReverseZeroPaddedString() {
        if (wildcardLoc > -1) {
            return toString(toReverseBytes(), true, 3 - this.wildcardLoc, this.numOctets, true);
        } else {
            return toString(toReverseBytes(), true, this.wildcardLoc, this.numOctets, true);
        }
    }
    
    @Override
    public IpAddress getStartIp(int validBits) {
        byte[] ipaddress = new byte[4];
        for (int i = 0; i < 4; i++) {
            if (validBits < 0) {
                // Do nothing
            } else if (validBits < 8) {
                int shift = 8 - validBits;
                ipaddress[i] = (byte) (((0x00FF >>> shift) << shift) & this.ipaddress[i]);
            } else {
                ipaddress[i] = this.ipaddress[i];
            }
            validBits -= 8;
        }
        return new IpV4Address(ipaddress);
    }
    
    @Override
    public IpAddress getEndIp(int validBits) {
        byte[] ipaddress = new byte[4];
        for (int i = 0; i < 4; i++) {
            if (validBits < 0) {
                ipaddress[i] = (byte) (0x00FF);
            } else if (validBits < 8) {
                ipaddress[i] = (byte) ((0x00FF >>> validBits) | this.ipaddress[i]);
            } else {
                ipaddress[i] = this.ipaddress[i];
            }
            validBits -= 8;
        }
        return new IpV4Address(ipaddress);
    }
    
    @Override
    public int compareTo(IpAddress o) {
        if (o instanceof IpV4Address) {
            long i1 = toNumber();
            long i2 = ((IpV4Address) o).toNumber();
            if (i1 < i2) {
                return -1;
            } else if (i1 > i2) {
                return 1;
            }
        } else if (o instanceof IpV6Address) {
            IpV4Address addr = ((IpV6Address) o).toIpV4Address();
            if (addr == null) {
                return -1;
            } else {
                return compareTo(addr);
            }
        }
        return 0;
    }
    
    @Override
    public boolean equals(Object o) {
        IpV4Address other = null;
        if (o instanceof IpV6Address) {
            other = ((IpV6Address) o).toIpV4Address();
        } else if (o instanceof IpV4Address) {
            other = (IpV4Address) o;
        }
        if (null == other) {
            return false;
        }
        
        return Objects.equal(this.toNumber(), other.toNumber());
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        for (int i = 0; i < 4; i++) {
            hashCode += ipaddress[i];
        }
        return hashCode;
    }
    
}
