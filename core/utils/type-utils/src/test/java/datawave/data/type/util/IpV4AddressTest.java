package datawave.data.type.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class IpV4AddressTest {

    // Reverse the canonical octets without adding omitted octets or restoring a discarded wildcard suffix.
    @ParameterizedTest
    // @formatter:off
    @CsvSource({
            "1.2.3.4,       1.2.3.4,     001.002.003.004, 4.3.2.1,    004.003.002.001",
            "255.0.128.7,   255.0.128.7, 255.000.128.007, 7.128.0.255,007.128.000.255",
            "*,             *,           *,               *,          *",
            "1.*,           1.*,         001.*,           *.1,        *.001",
            "1.2.*,         1.2.*,       001.002.*,       *.2.1,      *.002.001",
            "1.2.3.*,       1.2.3.*,     001.002.003.*,   *.3.2.1,    *.003.002.001",
            "*.4,           *.4,         *.004,           4.*,        004.*",
            "*.13.4,        *.13.4,      *.013.004,       4.13.*,     004.013.*",
            "*.2.13.4,      *.2.13.4,    *.002.013.004,   4.13.2.*,   004.013.002.*",
            "1.*.3.4,       1.*,         001.*,           *.1,        *.001",
            "1.2.*.*,       1.2.*,       001.002.*,       *.2.1,      *.002.001",
            "1.2..*,        1.2.*,       001.002.*,       *.2.1,      *.002.001",
            "1.2.3..*,      1.2.3.*,     001.002.003.*,   *.3.2.1,    *.003.002.001",
            "*.2.13.4.5,    *.2.13.4,    *.002.013.004,   4.13.2.*,   004.013.002.*"
    })
    // @formatter:on
    public void testCanonicalReverse(String input, String forward, String padded, String reverse, String reversePadded) {
        IpV4Address address = IpV4Address.parse(input);
        assertEquals(forward, address.toString());
        assertEquals(padded, address.toZeroPaddedString());
        assertEquals(reverse, address.toReverseString());
        assertEquals(reversePadded, address.toReverseZeroPaddedString());
    }
}
