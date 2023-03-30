package datawave.query.function.util;

import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.SoftReferenceObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KryoObjectPool implements AutoCloseable {
    private final static Logger LOG = LoggerFactory.getLogger(KryoObjectPool.class);
    private final ObjectPool<KryoEntry> pool;
    
    public KryoObjectPool() {
        this.pool = new SoftReferenceObjectPool<>(new KryoObjectPoolFactory());
    }
    
    public KryoReference acquireObject() {
        try {
            KryoEntry ke = pool.borrowObject();
            if (LOG.isTraceEnabled()) {
                LOG.trace("Borrowing object: {}", System.identityHashCode(ke));
            }
            return new KryoReference(this, ke);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
    
    public void releaseObject(KryoReference ref) {
        try {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Returning object: {}", System.identityHashCode(ref.getEntry()));
            }
            pool.returnObject(ref.getEntry());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
    
    @Override
    public void close() {
        pool.close();
    }
}
