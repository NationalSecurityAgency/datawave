package nsa.datawave.data.type;

import nsa.datawave.data.normalizer.IpAddressNormalizer;
import nsa.datawave.data.normalizer.Normalizer;
import nsa.datawave.data.type.util.IpAddress;
import nsa.datawave.data.type.util.IpV4Address;
import nsa.datawave.data.type.util.IpV6Address;

public class IpAddressType extends BaseType<IpAddress> {
    
    private static final long serialVersionUID = -6512690642978201801L;
    
    public IpAddressType() {
        super(Normalizer.IP_ADDRESS_NORMALIZER);
    }
    
    public IpAddressType(String delegateString) {
        super(delegateString, Normalizer.IP_ADDRESS_NORMALIZER);
    }
    
    public String[] normalizeCidrToRange(String cidr) {
        return ((IpAddressNormalizer) normalizer).normalizeCidrToRange(cidr);
    }
    
    /**
     * calculate the size based on the type of ip address type this is. Do not include the normalizer except a reference
     * 
     * @return
     */
    @Override
    public long sizeInBytes() {
        long base = PrecomputedSizes.STRING_STATIC_REF + (2 * normalizedValue.length());
        long ipSize;
        if (delegate instanceof IpV4Address) {
            ipSize = PrecomputedSizes.IPV4ADDRESS_STATIC_REF;
        } else if (delegate instanceof IpV6Address) {
            ipSize = PrecomputedSizes.IPV6ADDRESS_STATIC_REF;
        } else {
            // let the sizer figure it out
            ipSize = Sizer.getObjectSize(delegate) + Sizer.REFERENCE;
        }
        return base + ipSize + Sizer.REFERENCE;
    }
}
