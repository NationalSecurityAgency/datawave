package datawave.typemetadata;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class TypeMetadataWriter {
    
    public static class Builder<B extends Builder<B>> {
        private TypeMetadataBridge bridge;
        
        protected B self() {
            return (B) this;
        }
        
        public B withBridge(TypeMetadataBridge bridge) {
            this.bridge = bridge;
            return self();
        }
        
        public TypeMetadataWriter build() {
            return new TypeMetadataWriter(this);
        }
    }
    
    public static Builder<?> builder() {
        return new Builder();
    }
    
    private static final Logger log = Logger.getLogger(TypeMetadataWriter.class);
    
    protected TypeMetadataBridge bridge;
    
    private TypeMetadataWriter() {}
    
    private TypeMetadataWriter(Builder<?> builder) {
        this(builder.bridge);
    }
    
    private TypeMetadataWriter(TypeMetadataBridge bridge) {
        this.bridge = bridge;
    }
    
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
            log.debug("table:" + name + " wrote the typeMetadataMap to hdfs at " + this.bridge.getUri() + " " + this.bridge.getDir() + "/" + name + "/"
                            + this.bridge.getTempFileName());
        } catch (Exception ex) {
            log.warn("table:" + name + " Unable to write typeMetadataMap", ex);
        } finally {
            if (oos != null) {
                oos.close();
            }
        }
        try {
            this.bridge.rename(name);
        } catch (Exception ex) {
            log.warn("table:" + name + " Unable to rename typeMetadataFile", ex);
        }
    }
    
    public static class Factory {
        public static final Logger log = Logger.getLogger(TypeMetadataProvider.Factory.class);
        
        private static TypeMetadataWriter typeMetadataWriter;
        
        public static synchronized TypeMetadataWriter createTypeMetadataWriter() {
            if (typeMetadataWriter != null)
                return typeMetadataWriter;
            
            ApplicationContext context = new AnnotationConfigApplicationContext("datawave.typemetadata");
            // ignore calls to close as this blows away the cache manager
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
