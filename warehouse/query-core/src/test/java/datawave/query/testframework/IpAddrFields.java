package datawave.query.testframework;

import datawave.query.testframework.IpAddressDataType.IpAddrField;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Defines the indexes for the {@link IpAddressDataType}.
 */
public class IpAddrFields extends AbstractFields {
    
    private static final Collection<String> index = Arrays.asList(IpAddrField.PUBLIC_IP.name(), IpAddrField.PRIVATE_IP.name(), IpAddrField.LOCATION.name(),
                    IpAddrField.PLANET.name());
    private static final Collection<String> indexOnly = new HashSet<>();
    private static final Collection<String> reverse = new HashSet<>();
    private static final Collection<String> multivalue = Arrays.asList(IpAddrField.PUBLIC_IP.name(), IpAddrField.PLANET.name());
    
    private static final Collection<Set<String>> composite = new HashSet<>();
    private static final Collection<Set<String>> virtual = new HashSet<>();
    
    static {
        reverse.addAll(index);
        // add composite and virtual values
    }
    
    public IpAddrFields() {
        super(index, indexOnly, reverse, multivalue, composite, virtual);
    }
    
    @Override
    public String toString() {
        return "IpAddrFields{" + super.toString() + "}";
    }
}
