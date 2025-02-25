/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package datawave.data.normalizer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import datawave.data.type.util.IpV4Address;

/**
 * 
 */
public class IpAddressNormalizerTest {
    private static Logger log = Logger.getLogger(IpAddressNormalizerTest.class);
    
    @Test
    public void testIpNormalizer01() {
        String ip = "1.2.3.4";
        String expected = "001.002.003.004";
        IpAddressNormalizer norm = new IpAddressNormalizer();
        String result = norm.normalize(ip);
        assertEquals(expected, result);
        log.debug("result: " + result);
    }
    
    @Test
    public void testIpNormalizer02() {
        String ip = "1.2.3";
        IpAddressNormalizer norm = new IpAddressNormalizer();
        assertThrows(IllegalArgumentException.class, () -> norm.normalize(ip));
    }
    
    @Test
    public void testIpNormalizer03() {
        IpAddressNormalizer norm = new IpAddressNormalizer();
        if (log.isDebugEnabled()) {
            log.debug("testIpNormalizer03");
            log.debug(norm.normalize("1.2.3.*"));
            log.debug(norm.normalize("1.2.3..*"));
            log.debug(norm.normalize("1.2.*"));
            log.debug(norm.normalize("1.2..*"));
            log.debug(norm.normalize("1.*"));
            log.debug(norm.normalize("1..*"));
            
        }
        assertEquals("001.002.003.*", norm.normalize("1.2.3.*"));
        assertEquals("001.002.003.*", norm.normalize("1.2.3..*"));
        assertEquals("001.002.*", norm.normalize("1.2.*"));
        assertEquals("001.002.*", norm.normalize("1.2..*"));
        assertEquals("001.*", norm.normalize("1.*"));
        assertEquals("001.*", norm.normalize("1..*"));
    }
    
    @Test
    public void testIpNormalizer04() {
        log.debug("testIpNormalizer04");
        IpAddressNormalizer norm = new IpAddressNormalizer();
        log.debug(norm.normalize("*.2.13.4"));
        log.debug(norm.normalize("*.13.4"));
        assertEquals("*.002.013.004", norm.normalize("*.2.13.4"));
        assertEquals("*.013.004", norm.normalize("*.13.4"));
    }
    
    // TEST IS TURNED OFF
    @Test
    @Disabled
    public void testIpNormalizer05() {
        log.debug("testIpNormalizer05");
        IpV4Address ip = IpV4Address.parse("*.2.13.4");
        if (log.isDebugEnabled()) {
            log.debug(ip.toString());
            log.debug(ip.toZeroPaddedString());
            log.debug(ip.toReverseString());
            log.debug(ip.toReverseZeroPaddedString());
        }
    }
    
    /*
     * NOTE: call toReverseString() on a wildcarded ip doesn't work right although this is not much of an issue.
     */
    // TEST IS TURNED OFF
    @Test
    @Disabled
    public void testIpNormalizer06() {
        log.debug("testIpNormalizer06");
        IpV4Address ip = IpV4Address.parse("1.2.*");
        if (log.isDebugEnabled()) {
            log.debug(ip.toString());
            log.debug(ip.toZeroPaddedString());
            log.debug(ip.toReverseString());
            log.debug(ip.toReverseZeroPaddedString());
        }
    }
    
    @Test
    public void testIpNormalizer07() {
        log.debug("testIpNormalizer07");
        IpAddressNormalizer norm = new IpAddressNormalizer();
        log.debug(norm.normalize(" *.2. 13.4"));
        log.debug(norm.normalize(" *.13.4 "));
        assertEquals("*.002.013.004", norm.normalize(" *.2. 13.4"));
        assertEquals("*.013.004", norm.normalize(" *.13.4 "));
    }
    
    @Test
    public void testCidrTranslations() {
        log.debug("testCidrTranslations");
        IpAddressNormalizer norm = new IpAddressNormalizer();
        assertArrayEquals(norm.normalizeCidrToRange("1.2.3.4/32"), new String[] {"001.002.003.004", "001.002.003.004"});
        assertArrayEquals(norm.normalizeCidrToRange("1.2.3.0/24"), new String[] {"001.002.003.000", "001.002.003.255"});
        assertArrayEquals(norm.normalizeCidrToRange("1.2.0.0/16"), new String[] {"001.002.000.000", "001.002.255.255"});
        assertArrayEquals(norm.normalizeCidrToRange("1.0.0.0/8"), new String[] {"001.000.000.000", "001.255.255.255"});
        assertArrayEquals(norm.normalizeCidrToRange("1.2.3.4/30"), new String[] {"001.002.003.004", "001.002.003.007"});
        
    }
}
