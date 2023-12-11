package datawave.microservice.configcheck.util;

import java.io.File;
import java.nio.file.Path;

public class FileUtils {
    public static final String WORKING_DIR = "working.dir";
    public static final String workingDir = System.getProperty(WORKING_DIR);
    
    public static Path getFilePath(String file) {
        Path path = null;
        if (file != null) {
            if (new File(file).isAbsolute()) {
                path = Path.of(file);
            } else if (workingDir != null) {
                path = Path.of(workingDir, file);
            } else {
                path = Path.of(file);
            }
        }
        return path;
    }
    
    public static Path getFilePath(String parent, String file) {
        Path path = null;
        Path parentPath = getFilePath(parent);
        if (parentPath != null) {
            path = parentPath.resolve(file);
        }
        if (path == null || !path.toFile().exists()) {
            path = getFilePath(file);
        }
        return path;
    }
}
