package nsa.datawave.data.type;

import nsa.datawave.data.normalizer.IpAddressNormalizer;
import nsa.datawave.data.normalizer.Normalizer;
import nsa.datawave.data.type.util.IpAddress;

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
