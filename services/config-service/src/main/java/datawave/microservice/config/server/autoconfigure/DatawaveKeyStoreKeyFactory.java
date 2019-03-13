package datawave.microservice.config.server.autoconfigure;

import org.springframework.core.io.Resource;
import org.springframework.security.rsa.crypto.KeyStoreKeyFactory;
import org.springframework.util.StringUtils;

/**
 * Extends {@link KeyStoreKeyFactory} in order to change the type determination for PKCS21 files. The parent uses the filename extension, however the standard
 * extension for PKCS12 files is "p12" and there is no Java KeyStore with the type "p12", only "pkcs12". Therefore this class returns a "pkcs12" keystore type
 * if the file extension is "p12".
 */
public class DatawaveKeyStoreKeyFactory extends KeyStoreKeyFactory {
    public DatawaveKeyStoreKeyFactory(Resource resource, char[] password) {
        super(resource, password, type(resource));
    }
    
    public DatawaveKeyStoreKeyFactory(Resource resource, char[] password, String type) {
        super(resource, password, type);
    }
    
    private static String type(Resource resource) {
        String ext = StringUtils.getFilenameExtension(resource.getFilename());
        if ("p12".equalsIgnoreCase(ext))
            ext = "pkcs12";
        return ext == null ? "jks" : ext;
    }
}
