package datawave.query.util.sortedset;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.zip.GZIPInputStream;

public class SortedSetInputStream extends InputStream {
    // The passed in uncompressed stream
    private InputStream uncompressedStream = null;
    
    // The wrapping compressed stream
    private InputStream compressedStream = null;
    
    // The current stream delegate (either uncompressedStream or compressedStream)
    private ObjectInputStream delegate = null;
    
    // A flag denoting whether we are using the compressed or uncompressed variant
    private boolean compressed = false;
    
    public SortedSetInputStream(InputStream delegate) throws IOException {
        this(delegate, false);
    }
    
    public SortedSetInputStream(InputStream delegate, boolean compress) throws IOException {
        this.uncompressedStream = delegate;
        setCompressed(compress);
    }
    
    public boolean isCompressed() {
        return this.compressed;
    }
    
    public void setCompressed(boolean compress) throws IOException {
        if (this.compressed != compress || delegate == null) {
            this.delegate = new ObjectInputStream(compress ? getCompressedStream() : this.uncompressedStream);
            this.compressed = compress;
        }
    }
    
    private InputStream getCompressedStream() throws IOException {
        if (this.compressedStream == null) {
            this.compressedStream = new GZIPInputStream(uncompressedStream);
        }
        return this.compressedStream;
    }
    
    @Override
    public int read() throws IOException {
        return this.delegate.read();
    }
    
    @Override
    public int read(byte[] b) throws IOException {
        return this.delegate.read(b);
    }
    
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return this.delegate.read(b, off, len);
    }
    
    public Object readObject() throws IOException, ClassNotFoundException {
        return this.delegate.readObject();
    }
    
    public int readInt() throws IOException {
        return this.delegate.readInt();
    }
    
    @Override
    public long skip(long n) throws IOException {
        return this.delegate.skip(n);
    }
    
    @Override
    public int available() throws IOException {
        return this.delegate.available();
    }
    
    @Override
    public void close() throws IOException {
        this.delegate.close();
    }
    
    @Override
    public synchronized void mark(int readlimit) {
        this.delegate.mark(readlimit);
    }
    
    @Override
    public synchronized void reset() throws IOException {
        this.delegate.reset();
    }
    
    @Override
    public boolean markSupported() {
        return this.delegate.markSupported();
    }
}
