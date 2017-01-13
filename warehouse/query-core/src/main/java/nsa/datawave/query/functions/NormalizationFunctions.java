package nsa.datawave.query.functions;

import nsa.datawave.data.type.IpAddressType;
import nsa.datawave.data.normalizer.NormalizationException;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 *
 */
@Deprecated
public class NormalizationFunctions {
    private static final Logger log = Logger.getLogger(NormalizationFunctions.class);
    
    public static String ipv4(String ip) throws NormalizationException {
        
        int index = ip.indexOf("..*");
        // is it a wildcard?
        if (index != -1) {
            String temp = ip.substring(0, index);
            if (log.isDebugEnabled()) {
                log.debug("wildcard ip: " + temp);
            }
            
            // zero padd temp and return with the .* on the end
            
            String[] octets = StringUtils.split(temp, ".");
            for (int i = 0; i < octets.length; i++) {
                int oct = Integer.parseInt(octets[i]);
                octets[i] = String.format("%03d", oct);
                if (log.isDebugEnabled()) {
                    log.debug("octets[i]: " + octets[i] + "  oct: " + oct);
                }
            }
            
            ip = StringUtils.join(octets, ".") + "..*";
            
        } else {
            IpAddressType normalizer = new IpAddressType();
            ip = normalizer.normalize(ip);
        }
        return ip;
    }
}
