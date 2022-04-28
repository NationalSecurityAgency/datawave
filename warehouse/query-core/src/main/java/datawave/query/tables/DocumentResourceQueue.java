package datawave.query.tables;

import com.google.common.base.Preconditions;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Closable queue that defines a simplistic factory that creates and destroys scanner resources.
 */
public final class DocumentResourceQueue implements Closeable {
    
    private static final Logger log = Logger.getLogger(DocumentResourceQueue.class);
    
    private final GenericObjectPool<DocumentResource> scannerPool;
    
    private final byte type;
    
    /**
     * Constructor for the queue that accepts the capacity and the connector. Defaults to the block when exhausted queue option
     * 
     * @param capacity
     * @param client
     */
    public DocumentResourceQueue(int capacity, AccumuloClient client) {
        this(capacity, client, GenericObjectPool.WHEN_EXHAUSTED_BLOCK);
    }
    
    /**
     * Constructor that accepts the type of pool, the connector, and the capacity
     * 
     * @param capacity
     * @param client
     * @param type
     */
    public DocumentResourceQueue(int capacity, AccumuloClient client, byte type) {
        Preconditions.checkNotNull(client);
        Preconditions.checkArgument(capacity > 0);
        
        this.type = type;
        
        PoolableObjectFactory<DocumentResource> factory = new DocumentResourceFactory(client);
        
        this.scannerPool = new GenericObjectPool<>(factory);
        // set the max capacity
        this.scannerPool.setMaxActive(capacity);
        // amount of time to wait for a connection
        this.scannerPool.setMaxWait(5000);
        // block
        this.scannerPool.setWhenExhaustedAction(type);
    }
    
    public DocumentResource getScannerResource() throws Exception {
        // let's grab an object from the pool,
        DocumentResource resource = null;
        while (resource == null) {
            try {
                resource = scannerPool.borrowObject();
            } catch (NoSuchElementException nse) {
                if (type == GenericObjectPool.WHEN_EXHAUSTED_FAIL) {
                    throw nse;
                }
            }
        }
        return resource;
    }
    
    /**
     * Closes the scanner resource, and returns the object to the pool
     * 
     * @param resource
     * @throws Exception
     */
    public void close(final DocumentResource resource) throws Exception {
        resource.close();
        scannerPool.returnObject(resource);
    }
    
    public int getCapacity() {
        return this.scannerPool.getMaxActive();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.io.Closeable#close()
     */
    @Override
    public synchronized void close() throws IOException {
        // let the currently running scanners go
        try {
            scannerPool.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
    
    private static final class DocumentResourceFactory implements PoolableObjectFactory<DocumentResource> {
        
        private final AccumuloClient client;
        
        DocumentResourceFactory(AccumuloClient client) {
            this.client = client;
        }
        
        @Override
        public void activateObject(DocumentResource object) {
            /* no-op */
        }
        
        @Override
        public void destroyObject(DocumentResource object) {
            if (log.isTraceEnabled())
                log.trace("Removing " + object.hashCode());
        }
        
        @Override
        public DocumentResource makeObject() {
            DocumentResource scannerResource = new DocumentResource(client);
            if (log.isTraceEnabled())
                log.trace("Returning " + scannerResource.hashCode());
            return scannerResource;
        }
        
        @Override
        public void passivateObject(DocumentResource object) throws Exception {
            destroyObject(object);
        }
        
        @Override
        public boolean validateObject(DocumentResource object) {
            return true;
        }
    }
}
