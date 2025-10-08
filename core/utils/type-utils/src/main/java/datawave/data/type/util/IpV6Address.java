package datawave.data.type.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.datasketches.common.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

/**
 * The IpV6 address
 *
 */
public class IpV6Address extends IpAddress {

    private static final Logger log = LoggerFactory.getLogger(IpV6Address.class);

    private static final long serialVersionUID = -1528748156190096213L;
    private final short[] ipaddress = new short[8];

    public IpV6Address(short[] address) {
        if (address.length != 8) {
            throw new IllegalArgumentException("An IpV6 address must be 8 shorts in length");
        }
        System.arraycopy(address, 0, this.ipaddress, 0, address.length);
    }

    /**
     * Return the underlying short values
     *
     * @return the IpV6 address short values
     */
    public short[] toShorts() {
        return new short[] {this.ipaddress[0], this.ipaddress[1], this.ipaddress[2], this.ipaddress[3], this.ipaddress[4], this.ipaddress[5], this.ipaddress[6],
                this.ipaddress[7]};
    }

    /**
     * Return the underlying short values in reverse order
     *
     * @return the IpV6 address short values in reverse order
     */
    public short[] toReverseShorts() {
        return new short[] {this.ipaddress[7], this.ipaddress[6], this.ipaddress[5], this.ipaddress[4], this.ipaddress[3], this.ipaddress[2], this.ipaddress[1],
                this.ipaddress[0]};
    }

    /**
     * Parse an address assume the specified base
     *
     * @param address
     *            the address
     * @return the IpV6 address
     * @throws IllegalArgumentException
     *             if the base is not 10, 8, 16, or the address cannot be parsed using the specified base or dotted/not
     */
    public static IpV6Address parse(String address) {
        String[] parts = Iterables.toArray(Splitter.on(':').split(address), String.class);
        if (parts.length > 8) {
            throw new IllegalArgumentException("Expected no more than 8 parts but got " + parts.length + " for " + address);
        }
        // if less than 8 parts, then there must be a "::" somewhere in there or an IPv4 address at the end
        boolean expectFiller = (address.contains("::"));
        boolean expectIpv4 = (address.indexOf('.') >= 0);
        if (!expectFiller) {
            if (expectIpv4 && parts.length != 7) {
                throw new IllegalArgumentException("Wrong number of sections in " + address);
            }
        } else {
            if (expectIpv4 && parts.length > 7) {
                throw new IllegalArgumentException("Wrong number of sections in " + address);
            }
        }

        short[] ipaddress = new short[8];
        int index = 0;
        for (int i = 0; i < 8; i++) {
            if (index >= parts.length)
                throw new IllegalArgumentException("Error processing address " + address);
            if (i == 6 && expectIpv4) {
                byte[] bytes = IpV4Address.parse(parts[index]).toBytes();
                ipaddress[i++] = (short) (((0x00FF & bytes[0]) << 8) | (0x00FF & bytes[1]));
                ipaddress[i] = (short) (((0x00FF & bytes[2])) << 8 | (0x00FF & bytes[3]));
            } else if (parts[index].isEmpty() && expectFiller) {
                i += (8 - parts.length);
                if (expectIpv4) {
                    i--;
                }
                // can only have one of these
                expectFiller = false;
            } else {
                int value = (!parts[index].isEmpty() ? Integer.parseInt(parts[index], 16) : 0);
                if ((value >>> 16) != 0) {
                    throw new IllegalArgumentException("Part " + parts[i] + " of " + address + " is out of range in base 16");
                }
                ipaddress[i] = (short) value;
            }
            index++;

        }
        return new IpV6Address(ipaddress);
    }

    public static String toString(short[] address, boolean zeroPadded, boolean skipZeros) {

        Preconditions.checkArgument(zeroPadded != skipZeros, "cannot zero pad address and skip zeros");
        // first convert to list of strings
        List<String> hextets = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < address.length; i++) {
            sb.setLength(0);
            String value = Integer.toString(0x00FFFF & address[i], 16);
            if (zeroPadded) {
                for (int j = value.length(); j < 4; j++) {
                    sb.append('0');
                }
            }
            sb.append(value);
            hextets.add(sb.toString());
        }

        // now find the longest run of all-zero hextets, first find breaks ties
        int zeroHextetStart = -1;
        int zeroHextetLength = -1;
        if (skipZeros) {
            for (int i = 0; i < hextets.size(); i++) {
                if (hextets.get(i).equals("0")) {
                    int len = 1;
                    for (int j = i + 1; j < hextets.size(); j++) {
                        if (hextets.get(j).equals("0")) {
                            len++;
                        } else {
                            break;
                        }
                    }

                    if (len >= 2 && len > zeroHextetLength) {
                        zeroHextetStart = i;
                        zeroHextetLength = len;
                        log.trace("found sequence at index: {} length: {}", zeroHextetStart, zeroHextetLength);
                        i += (len - 1); // do not look at zero hextets that were already skipped
                    }
                }
            }
        }

        // now build the final address, replacing the longest run of zeroes if necessary
        return buildAddress(hextets, zeroHextetStart, zeroHextetLength);
    }

    /**
     * Build the final IPv6 address. If a run of all-zero hextets was detected and skipping zeroes was requested then a compressed address will be built.
     *
     * @param hextets
     *            the list of address components
     * @param zeroHextetStart
     *            the start index of the longest run of all-zero hextets
     * @param zeroHextetLength
     *            the length of the longest run of all-zero hextets
     * @return an IPv6 address
     */
    private static String buildAddress(List<String> hextets, int zeroHextetStart, int zeroHextetLength) {
        // now build the final address, replacing the longest run of zeroes if necessary
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hextets.size(); i++) {
            if (i == zeroHextetStart) {
                sb.append("::");
                i += (zeroHextetLength - 1);
            } else {
                sb.append(hextets.get(i));
                if (i != hextets.size() - 1 && (i + 1 != zeroHextetStart)) {
                    // append delimiter if not at start and not before an all zero start
                    sb.append(":");
                }
            }
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return toString(ipaddress, false, true);
    }

    @Override
    public String toZeroPaddedString() {
        return toString(ipaddress, true, false);
    }

    @Override
    public String toReverseString() {
        return toString(toReverseShorts(), false, true);
    }

    @Override
    public String toReverseZeroPaddedString() {
        return toString(toReverseShorts(), true, false);
    }

    /**
     * Return the IpV4Address representation if only the last 2 shorts are set
     *
     * @return the IpV4Address representation, null if not compatible with IpV4
     */
    public IpV4Address toIpV4Address() {
        if (ipaddress[0] != 0 || ipaddress[1] != 0 || ipaddress[2] != 0 || ipaddress[3] != 0 || ipaddress[4] != 0 || ipaddress[5] != 0) {
            return null;
        } else {
            return new IpV4Address(((0x00FFFFl & ipaddress[6]) << 16) | (0x00FFFFl & ipaddress[7]));
        }
    }

    @Override
    public IpAddress getStartIp(int validBits) {
        short[] ipaddress = new short[8];
        for (int i = 0; i < 8; i++) {
            if (validBits < 0) {
                // Do nothing
            } else if (validBits < 16) {
                int shift = 16 - validBits;
                ipaddress[i] = (short) (((0x00FFFF >>> shift) << shift) & this.ipaddress[i]);
            } else {
                ipaddress[i] = this.ipaddress[i];
            }
            validBits -= 16;
        }
        return new IpV6Address(ipaddress);
    }

    @Override
    public IpAddress getEndIp(int validBits) {
        short[] ipaddress = new short[8];
        for (int i = 0; i < 8; i++) {
            if (validBits < 0) {
                ipaddress[i] = (short) (0x00FFFF);
            } else if (validBits < 16) {
                ipaddress[i] = (short) ((0x00FFFF >>> validBits) | this.ipaddress[i]);
            } else {
                ipaddress[i] = this.ipaddress[i];
            }
            validBits -= 16;
        }
        return new IpV6Address(ipaddress);
    }

    @Override
    public int compareTo(IpAddress o) {
        if (o instanceof IpV6Address) {
            IpV6Address other = (IpV6Address) o;
            return compareToIpV6Address(other);
        } else {
            IpV4Address addr = toIpV4Address();
            if (addr == null) {
                return 1;
            } else {
                return addr.compareTo((IpV4Address) o);
            }
        }
    }

    private int compareToIpV6Address(IpV6Address other) {
        for (int i = 0; i < 8; i++) {
            int comparison = compareSegments(ipaddress[i], other.ipaddress[i]);
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }

    private int compareSegments(short x, short y) {
        return (0x00FFFF & x) - (0x00FFFF & y);
    }

    @Override
    @SuppressFBWarnings(value = "EQ_CHECK_FOR_OPERAND_NOT_COMPATIBLE_WITH_THIS", justification = "this has been well tested")
    public boolean equals(Object o) {
        if (o instanceof IpV6Address) {
            return 0 == compareToIpV6Address(((IpV6Address) o));
        } else if (o instanceof IpV4Address) {
            IpV4Address addr = this.toIpV4Address();
            if (addr == null) {
                return false;
            } else {
                return addr.equals(o);
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hashCode = 0;
        for (int i = 0; i < 8; i++) {
            hashCode += ipaddress[i];
        }
        return hashCode;
    }

}
