package datawave.query.tables;

import java.io.Closeable;
import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.accumulo.core.client.Connector;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * Closable queue that defines a simplistic factory that creates and destroys scanner resources.
 */
public final class ResourceQueue implements Closeable {
    
    private static final Logger log = Logger.getLogger(ResourceQueue.class);
    
    private final GenericObjectPool<AccumuloResource> scannerPool;
    
    private final byte type;
    
    /**
     * Constructor for the queue that accepts the capacity and the connector. Defaults to the block when exhausted queue option
     * 
     * @param capacity
     *            the capacity
     * @param cxn
     *            a connector
     */
    public ResourceQueue(int capacity, Connector cxn) {
        this(capacity, cxn, GenericObjectPool.WHEN_EXHAUSTED_BLOCK);
    }
    
    /**
     * Constructor that accepts the type of pool, the connector, and the capacity
     *
     * @param capacity
     *            the capacity
     * @param cxn
     *            a connector
     * @param type
     *            type of pool
     */
    public ResourceQueue(int capacity, Connector cxn, byte type) {
        Preconditions.checkNotNull(cxn);
        Preconditions.checkArgument(capacity > 0);
        
        this.type = type;
        
        PoolableObjectFactory<AccumuloResource> factory = new AccumuloResourceFactory(cxn);
        
        this.scannerPool = new GenericObjectPool<AccumuloResource>(factory);
        // set the max capacity
        this.scannerPool.setMaxActive(capacity);
        // amount of time to wait for a connection
        this.scannerPool.setMaxWait(5000);
        // block
        this.scannerPool.setWhenExhaustedAction(type);
    }
    
    public AccumuloResource getScannerResource() throws Exception {
        // let's grab an object from the pool,
        AccumuloResource resource = null;
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
     *            a resource
     * @throws Exception
     *             if there are issues
     */
    public void close(final AccumuloResource resource) throws Exception {
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
    
    private static final class AccumuloResourceFactory implements PoolableObjectFactory<AccumuloResource> {
        
        private final Connector connector;
        
        AccumuloResourceFactory(Connector connector) {
            this.connector = connector;
        }
        
        @Override
        public void activateObject(AccumuloResource object) throws Exception {
            /* no-op */
        }
        
        @Override
        public void destroyObject(AccumuloResource object) throws Exception {
            if (log.isTraceEnabled())
                log.trace("Removing " + object.hashCode());
        }
        
        @Override
        public AccumuloResource makeObject() throws Exception {
            AccumuloResource scannerResource = new AccumuloResource(connector);
            if (log.isTraceEnabled())
                log.trace("Returning " + scannerResource.hashCode());
            return scannerResource;
        }
        
        @Override
        public void passivateObject(AccumuloResource object) throws Exception {
            destroyObject(object);
        }
        
        @Override
        public boolean validateObject(AccumuloResource object) {
            return true;
        }
    }
}
