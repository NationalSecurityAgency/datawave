package nsa.datawave.poller.manager;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * This file combining poll manager will add headers and trailers to the file and messages.
 * 
 */
public class MessageWrappingFileCombiningPollManager extends FileCombiningPollManager {
    private static final Logger log = Logger.getLogger(MessageWrappingFileCombiningPollManager.class);
    private Option fileHeaderOption, fileTrailerOption, msgHeaderOption, msgTrailerOption, stripHeaderOption, stripTrailerOption;
    private byte[] fileHeader, fileTrailer, msgHeader, msgTrailer, stripHeader, stripTrailer;
    
    /**
     * The default buffer size to use.
     */
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 2;
    
    /**
     * Create the header and trailer configuration options
     */
    @Override
    public Options getConfigurationOptions() {
        // Get options from super class
        Options o = super.getConfigurationOptions();
        
        // Add our own options
        fileHeaderOption = new Option("fh", "fileheader", true, "File header");
        fileHeaderOption.setRequired(false);
        fileHeaderOption.setValueSeparator(',');
        fileHeaderOption.setArgs(1);
        fileHeaderOption.setType(String.class);
        o.addOption(fileHeaderOption);
        
        fileTrailerOption = new Option("ft", "filetrailer", true, "File trailer");
        fileTrailerOption.setRequired(false);
        fileTrailerOption.setValueSeparator(',');
        fileTrailerOption.setArgs(1);
        fileTrailerOption.setType(String.class);
        o.addOption(fileTrailerOption);
        
        msgHeaderOption = new Option("mh", "msgheader", true, "Message header");
        msgHeaderOption.setRequired(false);
        msgHeaderOption.setValueSeparator(',');
        msgHeaderOption.setArgs(1);
        msgHeaderOption.setType(String.class);
        o.addOption(msgHeaderOption);
        
        msgTrailerOption = new Option("mt", "msgtrailer", true, "Message trailer");
        msgTrailerOption.setRequired(false);
        msgTrailerOption.setValueSeparator(',');
        msgTrailerOption.setArgs(1);
        msgTrailerOption.setType(String.class);
        o.addOption(msgTrailerOption);
        
        stripHeaderOption = new Option("sh", "stripheader", true, "Message header to strip");
        stripHeaderOption.setRequired(false);
        stripHeaderOption.setValueSeparator(',');
        stripHeaderOption.setArgs(1);
        stripHeaderOption.setType(String.class);
        o.addOption(stripHeaderOption);
        
        stripTrailerOption = new Option("st", "striptrailer", true, "Message trailer to strip");
        stripTrailerOption.setRequired(false);
        stripTrailerOption.setValueSeparator(',');
        stripTrailerOption.setArgs(1);
        stripTrailerOption.setType(String.class);
        o.addOption(stripTrailerOption);
        
        return o;
    }
    
    /**
     * Get the header and trailers from the command line.
     */
    @Override
    public void configure(CommandLine cl) throws Exception {
        super.configure(cl);
        // Get options
        fileHeader = getLastOptionValue(cl, fileHeaderOption.getOpt(), "").getBytes("UTF-8");
        fileTrailer = getLastOptionValue(cl, fileTrailerOption.getOpt(), "").getBytes("UTF-8");
        msgHeader = getLastOptionValue(cl, msgHeaderOption.getOpt(), "").getBytes("UTF-8");
        msgTrailer = getLastOptionValue(cl, msgTrailerOption.getOpt(), "").getBytes("UTF-8");
        stripHeader = getLastOptionValue(cl, stripHeaderOption.getOpt(), "").getBytes("UTF-8");
        stripTrailer = getLastOptionValue(cl, stripTrailerOption.getOpt(), "").getBytes("UTF-8");
    }
    
    /**
     * Calls super to set up new gzip output file in the local work directory, and adds the file header.
     */
    @Override
    protected void createNewOutputFile() throws IOException {
        super.createNewOutputFile();
        out.write(fileHeader);
    }
    
    /**
     * Do something with the file. Here it adds the message header, calls the super to copy the file, and then adds the message trailer.
     * 
     * @param inputFile
     * @param out
     * @return number of bytes processed
     */
    @Override
    protected long processFile(File inputFile, OutputStream out) throws Exception {
        InputStream in = new BufferedInputStream(new FileInputStream(inputFile));
        try {
            long bytes = 0;
            out.write(msgHeader);
            bytes += msgHeader.length;
            if (stripHeader.length == 0 && stripTrailer.length == 0) {
                bytes += IOUtils.copyLarge(in, out);
            } else {
                bytes += copyAndStrip(in, out, stripHeader, stripTrailer);
            }
            out.write(msgTrailer);
            bytes += msgTrailer.length;
            return bytes;
        } finally {
            IOUtils.closeQuietly(in);
        }
    }
    
    public static long copyAndStrip(InputStream input, OutputStream output, byte[] stripHeader, byte[] stripTrailer) throws IOException {
        int bufferSize = DEFAULT_BUFFER_SIZE;
        while (bufferSize < stripHeader.length || bufferSize < stripTrailer.length) {
            bufferSize += 1024;
        }
        byte[] lastBuffer = null;
        int lastBufferSize = 0;
        
        byte[] buffer = new byte[bufferSize];
        
        boolean strippedHeader = false;
        long count = 0;
        int n = 0;
        while (0 != (n = fillBuffer(input, buffer, 0))) {
            // if we have not already checked for the header, do so and remove as needed
            if (!strippedHeader) {
                if (stripHeader.length > 0 && startsWith(buffer, n, stripHeader)) {
                    System.arraycopy(buffer, stripHeader.length, buffer, 0, (n - stripHeader.length));
                    n -= stripHeader.length;
                    n += fillBuffer(input, buffer, n);
                }
                strippedHeader = true;
            }
            
            // if we have a trailer to check for, and the current buffer has less than the trailer bytes
            // then lets merge the previous buffer with this one, outputing the unneeded bytes
            if (stripTrailer.length > 0 && n < stripTrailer.length && lastBuffer != null) {
                int delta = stripTrailer.length - n;
                output.write(lastBuffer, 0, bufferSize - delta);
                count += bufferSize - delta;
                System.arraycopy(buffer, 0, buffer, delta, n);
                System.arraycopy(lastBuffer, bufferSize - delta, buffer, 0, delta);
                n += delta;
                // we no longer need the last buffer as this is the last one ever
                lastBuffer = null;
            }
            
            // now write the last buffer, make this buffer the last one
            if (lastBuffer != null) {
                output.write(lastBuffer, 0, lastBufferSize);
                count += lastBufferSize;
                
                byte[] temp = lastBuffer;
                lastBuffer = buffer;
                lastBufferSize = n;
                buffer = temp;
            } else {
                lastBuffer = buffer;
                lastBufferSize = n;
                buffer = new byte[bufferSize];
            }
        }
        
        // if we have a last buffer, then output removing trailer if needed
        if (lastBuffer != null) {
            if (stripTrailer.length > 0 && endsWith(lastBuffer, lastBufferSize, stripTrailer)) {
                output.write(lastBuffer, 0, lastBufferSize - stripTrailer.length);
                count += (lastBufferSize - stripTrailer.length);
            } else {
                output.write(lastBuffer, 0, lastBufferSize);
                count += lastBufferSize;
            }
        }
        
        return count;
    }
    
    public static int fillBuffer(InputStream input, byte[] buffer, int index) throws IOException {
        int count = 0;
        int n = 0;
        while ((count < (buffer.length - index)) && (-1 != (n = input.read(buffer, index + count, buffer.length - count - index)))) {
            count += n;
        }
        return count;
    }
    
    private static boolean startsWith(byte[] bytes, int length, byte[] test) {
        if (length < test.length) {
            return false;
        }
        int i = 0;
        for (; i < test.length; i++) {
            if (bytes[i] != test[i]) {
                break;
            }
        }
        return (i == test.length);
    }
    
    private static boolean endsWith(byte[] bytes, int length, byte[] test) {
        if (length < test.length) {
            return false;
        }
        int i = 0;
        for (; i < test.length; i++) {
            if (bytes[length - test.length + i] != test[i]) {
                break;
            }
        }
        return (i == test.length);
    }
    
    /**
     * Completes current file. This will add the file trailer, and then call super method to copy file to HDFS etc.
     * 
     * @throws IOException
     * 
     */
    @Override
    protected void finishCurrentFile(boolean closing) throws IOException {
        try {
            out.write(fileTrailer);
        } catch (IOException e) {
            log.error("Unable to add trailer to output file: " + currentOutGzipFile.getAbsolutePath());
        } finally {
            super.finishCurrentFile(closing);
        }
    }
}
