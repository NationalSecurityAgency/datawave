package datawave.data.type;

import datawave.data.normalizer.IpAddressNormalizer;
import datawave.data.normalizer.Normalizer;
import datawave.data.type.util.IpAddress;

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
    
}
