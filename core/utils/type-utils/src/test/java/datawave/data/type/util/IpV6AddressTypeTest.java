/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package datawave.data.type.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

/**
 * 
 */
public class IpV6AddressTypeTest {
    private static Logger log = Logger.getLogger(IpV6AddressTypeTest.class);
    
    private final String[] in = { //
            "2001:0db8:0000:0000:0000:ff00:0042:8329", //
            "2003:DEAD:BEEF:4DAD:23:46:bb:101", //
            "2000:FFFF:EEEE:DD:CC:0000:0000:0000", //
            "AAAA:BBBB:CCCC:DDDD:EEEE:FFFF:2222:0", //
            "ff02:0b00:0000:0000:0001:0000:0000:000a", //
            "0000:0000:0000:0000:0000:0000:0000:0001", //
            "0000:0000:0000:0000:0000:0000:0000:0000", //
            "0001:0000:0000:0000:0000:0000:0000:0000", //
    };
    private final String[] out = { //
            "2001:db8::ff00:42:8329", //
            "2003:dead:beef:4dad:23:46:bb:101", //
            "2000:ffff:eeee:dd:cc:", //
            "aaaa:bbbb:cccc:dddd:eeee:ffff:2222:", //
            "ff02:b00::0001:0:0:a", //
            "::1", //
            "::", //
            "1::"
    
    };
    
    /**
     * Take a valid IpV6Address string, parse it to an IpV6Address instance, take the toString value from the IpV6Address and parse that into another
     * IpV6Address instance. Make sure that the toString for the original and reparsed addresses match.
     */
    @Test
    public void testIpNormalizer01() {
        
        for (String address : in) {
            IpV6Address addr = IpV6Address.parse(address);
            log.debug(address + " parsed to: " + addr);
            IpV6Address reparsed = IpV6Address.parse(addr.toString());
            log.debug(address + " parsed to: " + addr + " and re-parsed to " + reparsed);
            assertEquals(reparsed.toString(), addr.toString(), "Had a problem re-parsing " + address);
        }
    }
}
