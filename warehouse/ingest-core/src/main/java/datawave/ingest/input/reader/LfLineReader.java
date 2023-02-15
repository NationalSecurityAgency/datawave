package datawave.ingest.input.reader;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

/**
 * A class that provides a line reader from an input stream.
 */
public class LfLineReader implements LineReader {
    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
    private static final byte LF = '\n';
    private byte[] buffer;
    // the number of bytes of real data in the buffer
    private int bufferLength = 0;
    // the current position in the buffer
    private int bufferPosn = 0;
    private InputStream in;
    private boolean newLineIncluded = false; // indicates in newline is included
    
    /**
     * Create a line reader that reads from the given stream using the default buffer-size (64k).
     * 
     * @param in
     *            The input stream
     */
    public LfLineReader(InputStream in) {
        this(in, DEFAULT_BUFFER_SIZE);
    }
    
    /**
     * Create a line reader that reads from the given stream using the <code>io.file.buffer.size</code> specified in the given <code>Configuration</code>.
     * 
     * @param in
     *            input stream
     * @param conf
     *            configuration
     * @throws IOException
     *             if there is a problem pulling the configuration
     */
    public LfLineReader(InputStream in, Configuration conf) throws IOException {
        this(in, conf.getInt(Properties.IO_FILE_BUFFER_SIZE, DEFAULT_BUFFER_SIZE));
    }
    
    /**
     * Create a line reader that reads from the given stream using the given buffer-size.
     * 
     * @param in
     *            The input stream
     * @param bufferSize
     *            Size of the read buffer
     */
    public LfLineReader(InputStream in, int bufferSize) {
        this.in = in;
        this.buffer = new byte[bufferSize];
    }
    
    /**
     * Close the underlying stream.
     * 
     * @throws IOException
     *             if there is an issue closing the stream
     */
    public void close() throws IOException {
        in.close();
    }
    
    public boolean isNewLineIncluded() {
        return newLineIncluded;
    }
    
    /**
     * Read from the InputStream into the given Text.
     * 
     * @param str
     *            the object to store the given line
     * @return the number of bytes read including the newline
     * @throws IOException
     *             if the underlying stream throws
     */
    public int readLine(Text str) throws IOException {
        return readLine(str, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }
    
    /**
     * Read from the InputStream into the given Text.
     * 
     * @param str
     *            the object to store the given line
     * @param maxLineLength
     *            the maximum number of bytes to store into str.
     * @return the number of bytes read including the newline
     * @throws IOException
     *             if the underlying stream throws
     */
    public int readLine(Text str, int maxLineLength) throws IOException {
        return readLine(str, maxLineLength, Integer.MAX_VALUE);
    }
    
    /**
     * Read one line from the InputStream into the given Text. A line can be terminated by '\n' (LF). EOF also terminates an otherwise unterminated line.
     * 
     * @param str
     *            the object to store the given line (without newline)
     * @param maxLineLength
     *            the maximum number of bytes to store into str; the rest of the line is silently discarded.
     * @param maxBytesToConsume
     *            the maximum number of bytes to consume in this call. This is only a hint, because if the line cross this threshold, we allow it to happen. It
     *            can overshoot potentially by as much as one buffer length.
     *            
     * @return the number of bytes read including the (longest) newline found.
     *            
     * @throws IOException
     *             if the underlying stream throws
     */
    public int readLine(Text str, int maxLineLength, int maxBytesToConsume) throws IOException {
        /*
         * We're reading data from in, but the head of the stream may be already buffered in buffer, so we have several cases: 1. No newline characters are in
         * the buffer, so we need to copy everything and read another buffer from the stream. 2. An unambiguously terminated line is in buffer, so we just copy
         * to str.
         */
        str.clear();
        int txtLength = 0; // tracks str.getLength(), as an optimization
        int newlineLength = 0; // length of terminating newline
        long bytesConsumed = 0;
        do {
            int startPosn = bufferPosn; // starting from where we left off the
                                        // last time
            if (bufferPosn >= bufferLength) {
                startPosn = bufferPosn = 0;
                bufferLength = in.read(buffer);
                if (bufferLength <= 0)
                    break; // EOF
            }
            for (; bufferPosn < bufferLength; ++bufferPosn) { // search for
                                                              // newline
                if (buffer[bufferPosn] == LF) {
                    newlineLength = 1;
                    ++bufferPosn; // at next invocation proceed from following
                                  // byte
                    break;
                }
            }
            int readLength = bufferPosn - startPosn;
            bytesConsumed += readLength;
            int appendLength;
            if (isNewLineIncluded()) {
                appendLength = readLength;
            } else {
                appendLength = readLength - newlineLength;
            }
            if (appendLength > maxLineLength - txtLength) {
                appendLength = maxLineLength - txtLength;
            }
            if (appendLength > 0) {
                str.append(buffer, startPosn, appendLength);
                txtLength += appendLength;
            }
        } while (newlineLength == 0 && bytesConsumed < maxBytesToConsume);
        
        if (bytesConsumed > Integer.MAX_VALUE)
            throw new IOException("Too many bytes before newline: " + bytesConsumed);
        return (int) bytesConsumed;
    }
    
    public void setNewLineIncluded(boolean newLineIncluded) {
        this.newLineIncluded = newLineIncluded;
    }
    
}
