package nsa.datawave.data.type;

import nsa.datawave.data.normalizer.Normalizer;
import nsa.datawave.data.type.util.IpAddress;
import nsa.datawave.data.type.util.IpV4Address;
import nsa.datawave.data.type.util.IpV6Address;

public class IpV4AddressType extends BaseType<IpAddress> {
    
    private static final long serialVersionUID = 7214683578627273557L;
    
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
