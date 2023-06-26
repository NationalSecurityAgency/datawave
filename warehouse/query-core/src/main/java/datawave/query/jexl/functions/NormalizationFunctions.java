package datawave.query.jexl.functions;

import java.util.Collection;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

import datawave.data.normalizer.NormalizationException;
import datawave.data.type.IpAddressType;
import datawave.query.attributes.ValueTuple;

/**
 *
 */
public class NormalizationFunctions {
    private static final Logger log = Logger.getLogger(NormalizationFunctions.class);
    public static final String NORMALIZATION_FUNCTION_NAMESPACE = "normalize";

    public static String ipv4(Object ipField) throws NormalizationException {
        String ip = ValueTuple.getNormalizedStringValue(ipField);

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

    public static Collection<String> ipv4(Iterable<?> ips) throws NormalizationException {
        TreeSet<String> normalizedIps = Sets.newTreeSet();
        for (Object ip : ips) {
            normalizedIps.add(ipv4(ip));
        }
        return normalizedIps;
    }
}
