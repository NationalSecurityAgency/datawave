package datawave.query.function.util;

import com.esotericsoftware.kryo.Kryo;

public class KryoReference implements AutoCloseable {
    private final KryoEntry ke;
    private KryoObjectPool pool;
    private boolean closed;
    
    KryoReference(KryoObjectPool pool, KryoEntry ke) {
        this.pool = pool;
        this.ke = ke;
    }
    
    public Kryo getKryo() {
        return ke.getKryo();
    }
    
    @Override
    public void close() {
        if (!closed) {
            ke.getKryo().reset();
            pool.releaseObject(this);
            closed = true;
        }
    }
    
    KryoEntry getEntry() {
        return ke;
    }
}
