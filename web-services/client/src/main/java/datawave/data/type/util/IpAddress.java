package datawave.data.type.util;

import java.io.Serializable;

/**
 * The general IpAddress
 * 
 */
public abstract class IpAddress implements Serializable, Comparable<IpAddress> {
    private static final long serialVersionUID = -8461591227664317046L;
    
    public abstract String toZeroPaddedString();
    
    public abstract String toReverseString();
    
    public abstract String toReverseZeroPaddedString();
    
    public abstract IpAddress getStartIp(int validBits);
    
    public abstract IpAddress getEndIp(int validBits);
    
    /**
     * Parse an address and return an appropriate representation
     * 
     * @param address
     * @return An IpV4 or IpV6 address
     */
    public static IpAddress parse(String address) {
        try {
            return IpV4Address.parse(address);
        } catch (IllegalArgumentException iae) {
            return IpV6Address.parse(address);
        }
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof IpAddress) {
            return (compareTo((IpAddress) o) == 0);
        } else {
            return false;
        }
    }
}
