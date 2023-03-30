package datawave.query.function.util;

import com.esotericsoftware.kryo.Kryo;

class KryoEntry {
    private final Kryo kryo;
    
    public KryoEntry(Kryo kryo) {
        this.kryo = kryo;
    }
    
    public Kryo getKryo() {
        return kryo;
    }
}
