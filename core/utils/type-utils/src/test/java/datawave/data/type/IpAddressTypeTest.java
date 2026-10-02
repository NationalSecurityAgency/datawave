/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package datawave.data.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

import datawave.data.type.util.IpV4Address;

public class IpAddressTypeTest {
    private static final Logger log = Logger.getLogger(IpAddressTypeTest.class);

    @Test
    public void testIpNormalizer01() {
        String ip = "1.2.3.4";
        String expected = "001.002.003.004";
        IpAddressType norm = new IpAddressType();
        String result = norm.normalize(ip);
        assertEquals(expected, result);
        log.debug("result: " + result);
    }

    @Test
    public void testIpNormalizer02() {
        String ip = "1.2.3";
        IpAddressType norm = new IpAddressType();
        assertThrows(IllegalArgumentException.class, () -> norm.normalize(ip));
    }

    @Test
    public void testIpNormalizer03() {
        IpAddressType norm = new IpAddressType();
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
        IpAddressType norm = new IpAddressType();
        log.debug(norm.normalize("*.2.13.4"));
        log.debug(norm.normalize("*.13.4"));
        assertEquals("*.002.013.004", norm.normalize("*.2.13.4"));
        assertEquals("*.013.004", norm.normalize("*.13.4"));
    }

    @Test
    public void testIpNormalizer05() {
        IpV4Address ip = IpV4Address.parse("*.2.13.4");
        assertEquals("*.2.13.4", ip.toString());
        assertEquals("*.002.013.004", ip.toZeroPaddedString());
        assertEquals("4.13.2.*", ip.toReverseString());
        assertEquals("004.013.002.*", ip.toReverseZeroPaddedString());
    }

    @Test
    public void testIpNormalizer06() {
        IpV4Address ip = IpV4Address.parse("1.2.*");
        assertEquals("1.2.*", ip.toString());
        assertEquals("001.002.*", ip.toZeroPaddedString());
        assertEquals("*.2.1", ip.toReverseString());
        assertEquals("*.002.001", ip.toReverseZeroPaddedString());
    }
}
