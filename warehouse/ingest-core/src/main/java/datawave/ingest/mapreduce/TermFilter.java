package datawave.ingest.mapreduce;

import java.nio.charset.StandardCharsets;

import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

/**
 * Bloom filter implementation that puts an arbitrary string into the sink.
 * 
 * @param <T>
 */
public class TermFilter<T> implements Funnel<T> {
    
    /**
     * 
     */
    private static final long serialVersionUID = 5683986812953416483L;
    
    @Override
    public void funnel(T from, PrimitiveSink into) {
        into.putString(from.toString(), StandardCharsets.UTF_8);
    }
    
}
