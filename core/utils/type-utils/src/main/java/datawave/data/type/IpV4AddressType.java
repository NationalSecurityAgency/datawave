package datawave.data.type;

import datawave.data.normalizer.Normalizer;
import datawave.data.type.util.IpAddress;
import datawave.data.type.util.IpV4Address;
import datawave.data.type.util.IpV6Address;

public class IpV4AddressType extends BaseType<IpAddress> {
    
    private static final long serialVersionUID = 7214683578627273557L;
    private static final long STATIC_SIZE = PrecomputedSizes.STRING_STATIC_REF + Sizer.REFERENCE;
    
    public IpV4AddressType() {
        super(Normalizer.IP_ADDRESS_NORMALIZER);
    }
    
    /**
     * one String + either IpV4Address or IpV6Address + reference
     * 
     * @return
     */
    @Override
    public long sizeInBytes() {
        long base = STATIC_SIZE + (2 * normalizedValue.length());
        long ipSize;
        if (delegate instanceof IpV4Address) {
            ipSize = PrecomputedSizes.IPV4ADDRESS_STATIC_REF;
        } else if (delegate instanceof IpV6Address) {
            ipSize = PrecomputedSizes.IPV6ADDRESS_STATIC_REF;
        } else {
            // let the sizer figure it out
            ipSize = Sizer.getObjectSize(delegate) + Sizer.REFERENCE;
        }
        return base + ipSize;
    }
}
