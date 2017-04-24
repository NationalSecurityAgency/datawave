package nsa.datawave.query.util;

import com.google.common.collect.Maps;
import org.apache.commons.vfs2.FileChangeEvent;
import org.apache.commons.vfs2.FileListener;
import org.apache.commons.vfs2.impl.DefaultFileMonitor;
import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This is created on the tservers. It provides a singleton map of auths to the appropriate TypeMetadata loaded by the TypeMetadataHelper and written to vfs.
 * This Monitors the file in vfs so that when the file is updated, the singleton {@code Map<Set<String>>,TypeMetadata>} is refreshed.
 *
 */
public class TypeMetadataProvider implements FileListener {
    
    private static final Logger log = Logger.getLogger(TypeMetadataProvider.class);
    
    protected TypeMetadataBridge bridge;
    
    private static final Object lock = new Object();
    
    protected String[] metadataTableNames;
    
    /**
     * our singleton for the map of auths to TypeMetadata
     */
    public static Map<Set<String>,TypeMetadata> typeMetadataMap = Collections.emptyMap(); // just in case it is never set to anything useful
    
    protected final AtomicBoolean needUpdate = new AtomicBoolean(true);
    protected final AtomicBoolean typeMetadataLoaded = new AtomicBoolean(false);
    
    protected long delay;
    
    protected Map<String,DefaultFileMonitor> monitors = Maps.newHashMap();
    
    public synchronized TypeMetadata getTypeMetadata(String metadataTableName, Set<String> authKey) {
        synchronized (lock) {
            this.reloadTypeMetadata(metadataTableName);
            TypeMetadata typeMetadata = TypeMetadataProvider.typeMetadataMap.get(authKey);
            if (typeMetadata != null) {
                log.debug("getTypeMetadata(" + authKey + ") returning " + typeMetadata);
                return typeMetadata;
            } else {
                log.debug("getTypeMetadata(" + authKey + ") returning empty TypeMetadata");
                return new TypeMetadata();
            }
        }
    }
    
    public long getDelay() {
        return delay;
    }
    
    public void setDelay(long delay) {
        this.delay = delay;
    }
    
    public TypeMetadataBridge getBridge() {
        return bridge;
    }
    
    public void setBridge(TypeMetadataBridge bridge) {
        this.bridge = bridge;
    }
    
    public String[] getMetadataTableNames() {
        return metadataTableNames;
    }
    
    public void setMetadataTableNames(String[] metadataTableNames) {
        this.metadataTableNames = metadataTableNames;
    }
    
    /**
     * set up the monitor so that when the file system data is changed, our singleton will be refreshed
     */
    public void init() {
        for (String metadataTableName : this.metadataTableNames) {
            DefaultFileMonitor monitor = new DefaultFileMonitor(this);
            try {
                monitor.setDelay(delay);
                monitor.setRecursive(false);
                monitor.addFile(this.bridge.getFileObject(metadataTableName));
                log.debug("monitoring " + this.bridge.getFileObject(metadataTableName));
                monitor.start();
                this.monitors.put(metadataTableName, monitor);
            } catch (Exception ex) {
                monitor.stop();
                throw new RuntimeException("Failed to create TypeMetadataProvider with " + this.bridge.getUri() + this.bridge.getDir() + "/"
                                + this.bridge.getFileName(), ex);
            }
        }
    }
    
    public void forceUpdate() {
        this.needUpdate.set(true);
        this.update();
    }
    
    @Override
    public void fileCreated(FileChangeEvent event) throws Exception {
        
        this.needUpdate.set(true);
        this.typeMetadataLoaded.set(false);
        long modTime = event.getFile().getContent().getLastModifiedTime();
        log.debug("TypeMetadata file created, modified at: " + modTime);
        
    }
    
    @Override
    public void fileDeleted(FileChangeEvent event) throws Exception {
        this.needUpdate.set(true);
        this.typeMetadataLoaded.set(false);
        log.debug("TypeMetadata file deleted");
        synchronized (lock) {
            TypeMetadataProvider.typeMetadataMap.clear();
        }
    }
    
    @Override
    public void fileChanged(FileChangeEvent event) throws Exception {
        
        this.needUpdate.set(true);
        this.typeMetadataLoaded.set(false);
        long modTime = event.getFile().getContent().getLastModifiedTime();
        log.debug("TypeMetadata file changed, modified at: " + modTime);
    }
    
    private void reloadTypeMetadata(String metadataTableName) {
        if (this.needUpdate.get() == true && this.typeMetadataLoaded.get() == false) {
            try {
                synchronized (lock) {
                    log.debug("reloading TypeMetadata");
                    ObjectInputStream ois = new ObjectInputStream(this.bridge.getFileObject(metadataTableName).getContent().getInputStream());
                    TypeMetadataProvider.typeMetadataMap = (Map<Set<String>,TypeMetadata>) ois.readObject();
                    
                    this.needUpdate.set(false);
                    this.typeMetadataLoaded.set(true);
                    if (log.isTraceEnabled()) {
                        log.trace("reloaded TypeMetadataProvider.typeMetadataMap =" + TypeMetadataProvider.typeMetadataMap);
                    }
                    ois.close();
                }
            } catch (IOException | ClassNotFoundException ex) {
                log.warn("Unable to reload typeMetadata. Current value is " + TypeMetadataProvider.typeMetadataMap);
            }
        } else {
            log.debug("reload of TypeMetadata was unneeded");
        }
    }
    
    public void close() {
        for (DefaultFileMonitor monitor : this.monitors.values()) {
            monitor.stop();
        }
    }
    
    protected synchronized void update() {
        for (String metadataTableName : this.metadataTableNames) {
            this.reloadTypeMetadata(metadataTableName);
        }
    }
    
    @Override
    public String toString() {
        return "TypeMetadataProvider{" + "bridge=" + bridge + ", needUpdate=" + needUpdate + ", typeMetadataLoaded=" + typeMetadataLoaded + ", delay=" + delay
                        + ", monitors=" + monitors + '}';
    }
    
    /**
     * this Factory for TypeMetadataProvider is designed to be used on the tservers, where there is a vfs-classloader
     */
    public static class Factory {
        public static final Logger log = Logger.getLogger(TypeMetadataProvider.Factory.class);
        
        private static TypeMetadataProvider typeMetadataProvider;
        
        public static synchronized TypeMetadataProvider createTypeMetadataProvider() {
            if (typeMetadataProvider != null)
                return typeMetadataProvider;
            ClassLoader thisClassLoader = TypeMetadataProvider.Factory.class.getClassLoader();
            
            // ignore calls to close as this blows away the cache manager
            ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext();
            try {
                // To prevent failure when this is run on the tservers:
                // The VFS ClassLoader has been created and has been made the current thread's context classloader, but its resource paths are empty at this
                // time.
                // The spring ApplicationContext will prefer the current thread's context classloader, so the spring context would fail to find
                // any classes or context files to load.
                // Instead, set the classloader on the ApplicationContext to be the one that is loading this class.
                // It is a VFSClassLoader that has the accumulo lib/ext jars set as its resources.
                // After setting the classloader, then set the config locations and refresh the context.
                context.setClassLoader(thisClassLoader);
                context.setConfigLocations("classpath:/TypeMetadataBridgeContext.xml", "classpath:/TypeMetadataProviderContext.xml");
                context.refresh();
                typeMetadataProvider = context.getBean("typeMetadataProvider", TypeMetadataProvider.class);
            } catch (Throwable t) {
                // got here because the VFSClassLoader on the tservers does not implement findResources
                // none of the spring wiring will work.
                log.warn("Could not load spring context files. got " + t);
            }
            
            return typeMetadataProvider;
        }
    }
    
}
