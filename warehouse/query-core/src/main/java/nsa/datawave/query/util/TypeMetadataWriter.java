package nsa.datawave.query.util;

import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class TypeMetadataWriter {
    
    private static final Logger log = Logger.getLogger(TypeMetadataWriter.class);
    
    protected TypeMetadataBridge bridge;
    
    public TypeMetadataBridge getBridge() {
        return bridge;
    }
    
    public void setBridge(TypeMetadataBridge bridge) {
        this.bridge = bridge;
    }
    
    /**
     *
     * @param map
     *            - the TypeMetadata mapped with auth collections as keys
     * @param name
     *            - the name of the file that the TypeMetadata came from (like DatawaveMetadata
     * @throws Exception
     */
    public void writeTypeMetadataMap(Map<Set<String>,TypeMetadata> map, String name) throws Exception {
        ObjectOutputStream oos = null;
        try {
            oos = new ObjectOutputStream(this.bridge.getOutputStream(name));
            oos.writeObject(map);
            log.debug("wrote the typeMetadataMap to hdfs at " + this.bridge.getUri() + " " + this.bridge.getDir() + "/" + name + "/"
                            + this.bridge.getTempFileName());
        } catch (Exception ex) {
            log.warn("Unable to write typeMetadataMap", ex);
        } finally {
            if (oos != null) {
                oos.close();
            }
        }
        try {
            this.bridge.rename(name);
        } catch (Exception ex) {
            log.warn("Unable to rename typeMetadataFile", ex);
        }
    }
    
    public static class Factory {
        public static final Logger log = Logger.getLogger(TypeMetadataProvider.Factory.class);
        
        private static TypeMetadataWriter typeMetadataWriter;
        
        public static synchronized TypeMetadataWriter createTypeMetadataWriter() {
            if (typeMetadataWriter != null)
                return typeMetadataWriter;
            ClassLoader thisClassLoader = TypeMetadataProvider.Factory.class.getClassLoader();
            
            // ignore calls to close as this blows away the cache manager
            ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("classpath:/TypeMetadataBridgeContext.xml",
                            "classpath:/TypeMetadataWriterContext.xml");
            try {
                typeMetadataWriter = context.getBean("typeMetadataWriter", TypeMetadataWriter.class);
            } catch (Throwable t) {
                // got here because the VFSClassLoader on the tservers does not implement findResources
                // none of the spring wiring will work.
                log.warn("Could not load spring context files. got " + t);
                t.printStackTrace();
            }
            
            return typeMetadataWriter;
        }
    }
}
