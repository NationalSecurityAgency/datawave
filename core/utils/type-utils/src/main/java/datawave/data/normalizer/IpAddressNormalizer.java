package datawave.data.normalizer;

import org.apache.commons.net.util.SubnetUtils;

import datawave.data.type.util.IpAddress;
import datawave.query.parser.JavaRegexAnalyzer;
import datawave.query.parser.JavaRegexAnalyzer.JavaRegexParseException;

public class IpAddressNormalizer extends AbstractNormalizer<IpAddress> {
    
    private static final long serialVersionUID = 8604032745289485764L;
    
    public String normalize(String fieldValue) {
        try {
            fieldValue = fieldValue.replaceAll(" ", "");
            return IpAddress.parse(fieldValue).toZeroPaddedString();
        } catch (IllegalArgumentException iae) {
            throw new IpAddressNormalizer.Exception("Failed to normalize " + fieldValue + " as an IP");
        }
    }
    
    /**
     * Note that we really cannot normalize the regex here, so the regex must work against the normalized and unnormalized forms.
     */
    public String normalizeRegex(String fieldRegex) {
        try {
            return new JavaRegexAnalyzer(fieldRegex).getZeroPadIpRegex();
        } catch (JavaRegexParseException jrpe) {
            throw new IllegalArgumentException("Failed to parse ip regex " + fieldRegex, jrpe);
        }
    }
    
    public String[] normalizeCidrToRange(String cidr) {
        SubnetUtils subnetUtils = new SubnetUtils(cidr);
        subnetUtils.setInclusiveHostCount(true);
        SubnetUtils.SubnetInfo info = subnetUtils.getInfo();
        return new String[] {normalize(info.getLowAddress()), normalize(info.getHighAddress())};
    }
    
    @Override
    public String normalizeDelegateType(IpAddress delegateIn) {
        return delegateIn.toZeroPaddedString();
    }
    
    @Override
    public IpAddress denormalize(String in) {
        return IpAddress.parse(in);
    }
    
    public static class Exception extends IllegalArgumentException {
        public Exception(String message) {
            super(message);
        }
    }
    
}
