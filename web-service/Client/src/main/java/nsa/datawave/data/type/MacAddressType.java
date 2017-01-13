package nsa.datawave.data.type;

import nsa.datawave.data.normalizer.Normalizer;

public class MacAddressType extends BaseType<String> {
    
    private static final long serialVersionUID = -6743560287574389073L;
    
    public MacAddressType() {
        super(Normalizer.MAC_ADDRESS_NORMALIZER);
    }
}
