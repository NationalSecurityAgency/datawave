package datawave.age.off.util;

import java.io.IOException;
import java.io.Writer;

public class IndentingDelegatingWriter extends Writer {
    private final Writer writer;
    private final String indentation;

    public IndentingDelegatingWriter(String indentation, Writer writer) {
        this.indentation = indentation;
        this.writer = writer;
    }

    @Override
    public void write(String line) throws IOException {
        this.writer.write(indentation + line);
    }

    @Override
    public void write(char[] cbuf, int off, int len) throws IOException {
        this.writer.write(cbuf, off, len);
    }

    @Override
    public void flush() throws IOException {
        this.writer.flush();
    }

    @Override
    public void close() throws IOException {
        this.writer.close();
    }
}
