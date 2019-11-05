package datawave.query.util.sortedset;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

public class SortedSetOutputStream extends OutputStream {
    
    // The passed in uncompressed stream
    private OutputStream uncompressedStream = null;
    
    // The wrapping compressed stream
    private OutputStream compressedStream = null;
    
    // The current stream delegate (either uncompressedStream or compressedStream)
    private ObjectOutputStream delegate = null;
    
    // A flag denoting whether we are using the compressed or uncompressed variant
    private boolean compressed = false;
    
    public SortedSetOutputStream(OutputStream delegate) throws IOException {
        this(delegate, false);
    }
    
    public SortedSetOutputStream(OutputStream delegate, boolean compress) throws IOException {
        this.uncompressedStream = delegate;
        setCompressed(compress);
    }
    
    public boolean isCompressed() {
        return this.compressed;
    }
    
    public void setCompressed(boolean compress) throws IOException {
        if (this.compressed != compress || delegate == null) {
            if (this.delegate != null) {
                this.delegate.flush();
            }
            this.delegate = new ObjectOutputStream(compress ? getCompressedStream() : this.uncompressedStream);
            this.compressed = compress;
        }
    }
    
    private OutputStream getCompressedStream() throws IOException {
        if (this.compressedStream == null) {
            this.compressedStream = new GZIPOutputStream(uncompressedStream, true);
        }
        return this.compressedStream;
    }
    
    @Override
    public void write(int b) throws IOException {
        this.delegate.write(b);
    }
    
    @Override
    public void write(byte[] b) throws IOException {
        this.delegate.write(b);
    }
    
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        this.delegate.write(b, off, len);
    }
    
    public void writeObject(Object obj) throws IOException {
        this.delegate.writeObject(obj);
    }
    
    public void writeInt(int i) throws Exception {
        this.delegate.writeInt(i);
    }
    
    @Override
    public void flush() throws IOException {
        this.delegate.flush();
    }
    
    @Override
    public void close() throws IOException {
        this.delegate.close();
        this.delegate = null;
        this.uncompressedStream = this.compressedStream = null;
    }
}
