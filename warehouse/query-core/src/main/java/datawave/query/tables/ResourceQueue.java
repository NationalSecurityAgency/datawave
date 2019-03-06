package datawave.query.tables;

import java.io.Closeable;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Queue;

import org.apache.accumulo.core.client.Connector;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Queues;

/**
 * Closable queue that defines a simplistic factory that creates and destroys scanner resources.
 */
public class ResourceQueue implements Closeable {
    
    /**
     * default connector.
     */
    protected Connector connector;
    
    /**
     * Object pool that contains the scanner resources we will return
     */
    GenericObjectPool scannerPool;
    
    protected boolean isOpen;
    
    protected PoolableObjectFactory factory;
    
    private int capacity;
    
    private byte type;
    
    private static final Logger log = Logger.getLogger(ResourceQueue.class);
    
    /**
     * Constructor for the queue that accepts the capacity and the connector. Defaults to the block when exhausted queue option
     * 
     * @param capacity
     * @param cxn
     * @throws Exception
     */
    public ResourceQueue(int capacity, Connector cxn) throws Exception {
        this(capacity, cxn, GenericObjectPool.WHEN_EXHAUSTED_BLOCK);
    }
    
    /**
     * Constructor that accepts the type of pool, the connector, and the capacity
     * 
     * @param capacity
     * @param cxn
     * @param type
     * @throws Exception
     */
    public ResourceQueue(int capacity, Connector cxn, byte type) throws Exception {
        Preconditions.checkNotNull(cxn);
        
        connector = cxn;
        
        isOpen = true;
        
        configurePool(capacity, type);
        
    }
    
    /**
     * Constructor that accepts the type of pool, the connector, and the capacity
     * 
     * @param capacity
     * @param type
     * @throws Exception
     */
    protected ResourceQueue(int capacity, byte type) throws Exception {
        
        connector = null;
        
        isOpen = true;
        
        configurePool(capacity, type);
        
    }
    
    /**
     * configure the pool
     * 
     * @throws Exception
     */
    protected void configurePool(int capacity, byte type) throws Exception {
        Preconditions.checkArgument(capacity > 0);
        
        factory = new ScannerQueueFactory(capacity);
        
        this.capacity = capacity;
        
        scannerPool = new GenericObjectPool(factory);
        // set the max capacity
        scannerPool.setMaxActive(capacity);
        // amount of time to wait for a connection
        scannerPool.setMaxWait(5000);
        // block
        scannerPool.setWhenExhaustedAction(type);
        
        this.type = type;
        
    }
    
    public AccumuloResource getScannerResource() throws Exception {
        // let's grab an object from the pool,
        
        AccumuloResource resource = null;
        while (resource == null) {
            try {
                resource = ((AccumuloResource) scannerPool.borrowObject());
            } catch (NoSuchElementException nse) {
                if (type == GenericObjectPool.WHEN_EXHAUSTED_FAIL) {
                    throw nse;
                }
                if (Thread.interrupted()) {
                    throw new InterruptedException("Thread is interrupted");
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
    public void close(final AccumuloResource resource) throws Exception {
        resource.close();
        scannerPool.returnObject(resource);
    }
    
    public int getCapacity() {
        return capacity;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.io.Closeable#close()
     */
    @Override
    public synchronized void close() throws IOException {
        
        isOpen = false;
        // let the currently running scanners go
        try {
            scannerPool.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
        
    }
    
    protected class ScannerQueueFactory implements PoolableObjectFactory {
        
        protected Queue<AccumuloResource> resource;
        
        public ScannerQueueFactory(int capacity) {
            resource = Queues.newConcurrentLinkedQueue();
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see org.apache.commons.pool.PoolableObjectFactory#activateObject(java. lang.Object)
         */
        @Override
        public void activateObject(Object object) throws Exception {
            Preconditions.checkArgument(object instanceof AccumuloResource);
            
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see org.apache.commons.pool.PoolableObjectFactory#destroyObject(java.lang .Object)
         */
        @Override
        public void destroyObject(Object object) throws Exception {
            Preconditions.checkArgument(object instanceof AccumuloResource);
            if (log.isTraceEnabled())
                log.trace("Removing " + object.hashCode());
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see org.apache.commons.pool.PoolableObjectFactory#makeObject()
         */
        @Override
        public Object makeObject() throws Exception {
            AccumuloResource scannerResource = new AccumuloResource(connector);
            if (log.isTraceEnabled())
                log.trace("Returning " + scannerResource.hashCode());
            return scannerResource;
            
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see org.apache.commons.pool.PoolableObjectFactory#passivateObject(java. lang.Object)
         */
        @Override
        public void passivateObject(Object object) throws Exception {
            destroyObject(object);
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see org.apache.commons.pool.PoolableObjectFactory#validateObject(java. lang.Object)
         */
        @Override
        public boolean validateObject(Object object) {
            return object instanceof AccumuloResource;
        }
        
    }
    
}
