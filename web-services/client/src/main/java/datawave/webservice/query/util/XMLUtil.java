package datawave.webservice.query.util;

import org.apache.xerces.util.XMLChar;

public class XMLUtil {
    private XMLUtil() {
        // prevent construction
    }
    
    public static boolean isValidXML(String s) {
        for (char c : s.toCharArray()) {
            try {
                if (XMLChar.isValid(c) == false) {
                    return false;
                }
            } catch (Exception e) {
                return false;
            }
        }
        return true;
    }
}
