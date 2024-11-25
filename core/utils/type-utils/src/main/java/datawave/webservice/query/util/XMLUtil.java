package datawave.webservice.query.util;

public class XMLUtil {
    private XMLUtil() {
        // prevent construction
    }
    
    public static boolean isValidXML(String s) {
        return s.codePoints().allMatch(XMLUtil::isValidXMLChar);
    }
    
    // XML 1.0 spec says the following are valid XML characters:
    // #x9 | #xA | #xD | #x20-#xD7FF | #xE000-#xFFFD | #x10000-#x10FFFF
    //
    // XML 1.1 spec says the following are valid XML characters:
    // #x1-#xD7FF | #xE000-#xFFFD | #x10000-#x10FFFF
    public static boolean isValidXMLChar(int c) {
        // @formatter:off
        return c == 0x9
            || c == 0xA
            || c == 0xD
            || (c >= 0x20 && c <= 0xD7FF)
            || (c >= 0xE000 && c <= 0xFFFD)
            || (c >= 0x10000 && c <= 0x10FFFF);
        // @formatter:on
    }
}
