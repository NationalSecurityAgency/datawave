package datawave.util.ssdeep;

import org.junit.Assert;
import org.junit.Test;

public class SSDeepEncodingTest {

    static final String[] testData = {"12288:002r/VG4GjeZHkwuPikQ7lKH5p5H9x1beZHkwulizQ1lK55pGxlXTd8zbW:002LVG4GjeZEXi37l6Br1beZEdic1lmu",
            "6144:02C3nq73v1kHGhs6y7ppFj93NRW6/ftZTgC6e8o4toHZmk6ZxoXb0ns:02C4cGCLjj9Swfj9koHEk6/Fns",
            "3072:02irbxzGAFYDMxud7fKg3dXVmbOn5u46Kjnz/G8VYrs123D6pIJLIOSP:02MKlWQ7Sg3d4bO968rm7JO",
            "48:1aBhsiUw69/UXX0x0qzNkVkydf2klA8a7Z35:155w69MXAlNkmkWTF5"};

    @Test
    public void encode() {
        SSDeepEncoding encoding = new SSDeepEncoding();
        for (String e : testData) {
            byte[] b = encoding.encode(e);
            String s = new String(b);
            Assert.assertEquals(e, s);
        }
    }

    @Test
    public void encodeToBytes() {
        SSDeepEncoding encoding = new SSDeepEncoding();
        for (String e : testData) {
            byte[] b = new byte[e.length()];
            encoding.encodeToBytes(e, b, 0);
            String s = new String(b);
            Assert.assertEquals(e, s);
        }
    }
}
