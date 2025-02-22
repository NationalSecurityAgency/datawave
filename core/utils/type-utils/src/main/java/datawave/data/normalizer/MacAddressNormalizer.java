package datawave.data.normalizer;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

public class MacAddressNormalizer extends AbstractNormalizer<String> {
    
    private static final long serialVersionUID = -2606365671421121859L;
    
    public String normalize(String fieldValue) {
        
        String mac = "";
        
        String parts[] = Iterables.toArray(Splitter.on(':').split(fieldValue), String.class);
        if (parts.length == 6) {
            // Verify it is padded ie.e 11:01:00:11:11:11
            // Return 11-01-00-11-11-11
            return StringUtils.join(padWithZeros(parts), "-");
        }
        
        parts = Iterables.toArray(Splitter.on('-').split(fieldValue), String.class);
        if (parts.length == 6) {
            
            // Verify it is padded ie.e 11-01-00-11-11-11
            // Return 11-01-00-11-11-11
            return StringUtils.join(padWithZeros(parts), "-");
        }
        
        // 6 bytes for a macaddr
        
        try {
            long lData = Long.parseLong(fieldValue, 16);
            
            if (!isMac(lData)) {
                throw new IllegalArgumentException("Failed to normalize " + fieldValue + " as a MAC");
            }
            
            for (int i = 0; i < 6; i++) {
                final String twoChars = Long.toHexString(lData & 0x00000000000000FFl);
                lData = lData >> 8;
                if (twoChars.length() == 1) {
                    mac = "0" + twoChars + mac;
                    
                } else {
                    mac = twoChars + mac;
                    
                }
                mac = "-" + mac;
            }
            return (mac.substring(1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Failed to normalize " + fieldValue + " as a MAC");
        }
    }
    
    /**
     * Note that we really cannot normalize the regex here, so the regex must work against the normalized and unnormalized forms.
     */
    public String normalizeRegex(String fieldRegex) {
        return fieldRegex;
    }
    
    public static boolean isMac(Long lData) {
        
        long mask = 0xFFFF000000000000l;
        
        if ((lData & mask) != 0)
            return false;
        
        return true;
        
    }
    
    public static boolean isMac(String value) {
        
        long lData;
        
        try {
            lData = Long.parseLong(value, 16);
            
        } catch (Exception e) {
            return false;
        }
        
        return isMac(lData);
        
    }
    
    private static String[] padWithZeros(String mac[]) {
        String padded[] = new String[mac.length];
        
        for (int i = 0; i < mac.length; i++) {
            if (mac[i].length() == 1) {
                padded[i] = "0" + mac[i];
            } else {
                padded[i] = new String(mac[i]);
                
            }
        }
        
        return padded;
        
    }
    
    @Override
    public String normalizeDelegateType(String delegateIn) {
        return normalize(delegateIn);
    }
    
    @Override
    public String denormalize(String in) {
        return in;
    }
}
