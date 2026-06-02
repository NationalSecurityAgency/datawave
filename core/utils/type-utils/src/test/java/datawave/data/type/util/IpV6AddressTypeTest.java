package datawave.data.type.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IpV6AddressTypeTest {

    private static final Logger log = LoggerFactory.getLogger(IpV6AddressTypeTest.class);

    //  @formatter:off
    private final String[] inputs = { //
            "2001:0db8:0000:0000:0000:ff00:0042:8329", //
            "2003:DEAD:BEEF:4DAD:0023:0046:00bb:0101", //
            "2000:FFFF:EEEE:00DD:00CC:0000:0000:0000", //
            "AAAA:BBBB:CCCC:DDDD:EEEE:FFFF:2222:0000", //
            "ff02:0b00:0000:0000:0001:0000:0000:000a", //
            "0000:0000:0000:0000:0000:0000:0000:0001", //
            "0001:0000:0000:0000:0000:0000:0000:0000", //
            "abcd:0000:0000:abcd:0000:0000:0000:abcd", //
            "abcd:0000:abcd:0000:0000:0000:0000:abcd", //
            "0000:0bcd:abcd:abcd:abcd:abcd:abcd:abcd",
            "abcd:abcd:0000:0000:abcd:0000:0000:abcd", // replace first run of zero hextets when there is a tie
    };
    private final String[] outputs = { //
            "2001:db8::ff00:42:8329", //
            "2003:dead:beef:4dad:23:46:bb:101", //
            "2000:ffff:eeee:dd:cc::", //
            "aaaa:bbbb:cccc:dddd:eeee:ffff:2222:0", //
            "ff02:b00::1:0:0:a", //
            "::1", //
            "1::",
            "abcd:0:0:abcd::abcd",
            "abcd:0:abcd::abcd",
            "0:bcd:abcd:abcd:abcd:abcd:abcd:abcd",
            "abcd:abcd::abcd:0:0:abcd"
    };
    //  @formatter:on

    /**
     * Take a valid IpV6Address string, parse it to an IpV6Address instance, take the toString value from the IpV6Address and parse that into another
     * IpV6Address instance. Make sure that the toString for the original and reparsed addresses match.
     */
    @Test
    public void testIpNormalizer01() {
        for (int i = 0; i < inputs.length; i++) {
            String input = inputs[i];
            String expected = outputs[i];
            assertRoundTrip(input, expected);
        }
    }

    @Test
    public void testRoundTripToString() {
        String input = "0000:0bcd:abcd:abcd:abcd:abcd:abcd:abcd";
        String expected = "0:bcd:abcd:abcd:abcd:abcd:abcd:abcd";
        assertRoundTrip(input, expected);
    }

    @Test
    public void testAllZeroAddress() {
        String original = "0000:0000:0000:0000:0000:0000:0000:0000";
        String expected = "::";
        assertRoundTrip(original, expected);
    }

    @Test
    public void testAllZeroHextetsReplacedAtStart() {
        String original = "0000:0000:abcd:abcd:abcd:abcd:abcd:abcd";
        String expected = "::abcd:abcd:abcd:abcd:abcd:abcd";
        assertRoundTrip(original, expected);
    }

    @Test
    public void testAllZeroHextetsReplacedAtEnd() {
        String original = "abcd:abcd:abcd:abcd:abcd:abcd:0000:0000";
        String expected = "abcd:abcd:abcd:abcd:abcd:abcd::";
        assertRoundTrip(original, expected);
    }

    @Test
    public void testAllZeroHextetsReplacedInMiddle() {
        String original = "abcd:abcd:abcd:0000:0000:abcd:abcd:abcd";
        String expected = "abcd:abcd:abcd::abcd:abcd:abcd";
        assertRoundTrip(original, expected);
    }

    @Test
    public void testTwoInstancesOfZeroHextetsOfEquivalentLengthsOnlyFirstIsReplaced() {
        String original = "abcd:0000:0000:abcd:0000:0000:abcd:abcd";
        String expected = "abcd::abcd:0:0:abcd:abcd";
        assertRoundTrip(original, expected);
    }

    @Test
    public void testLongerInstanceOfZeroHextetIsReplaced() {
        String original = "abcd:0000:0000:abcd:0000:0000:0000:abcd";
        String expected = "abcd:0:0:abcd::abcd";
        assertRoundTrip(original, expected);
    }

    @Test
    public void testInvalidToStringCall() {
        String zeroAddress = "0000:0000:0000:0000:0000:0000:0000:0000";
        IpV6Address address = IpV6Address.parse(zeroAddress);
        short[] shorts = address.toShorts();
        assertThrows(IllegalArgumentException.class, () -> IpV6Address.toString(shorts, true, true));
        assertThrows(IllegalArgumentException.class, () -> IpV6Address.toString(shorts, false, false));
    }

    private void assertRoundTrip(String original, String expected) {
        assertEquals(7, StringUtils.countMatches(original, ":"));

        IpV6Address address = IpV6Address.parse(original);
        String serialized = address.toString();
        assertEquals(expected, serialized);

        IpV6Address deserialized = IpV6Address.parse(serialized);
        assertEquals(expected, deserialized.toString());

        String zeroPadded = deserialized.toZeroPaddedString();
        assertEquals(original.toUpperCase(), zeroPadded.toUpperCase());
    }
}
