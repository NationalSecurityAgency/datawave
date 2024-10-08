package datawave.age.off.util;

import java.io.IOException;
import java.io.Writer;

public class IndentingDelegatingWriter extends Writer {
    private final Writer writer;
    private final String indentation;
    private boolean shouldIndentNextWrite;

    public IndentingDelegatingWriter(String indentation, Writer writer) {
        this.indentation = indentation;
        this.writer = writer;
        this.shouldIndentNextWrite = true;
    }

    @Override
    public void write(String line) throws IOException {
        if (this.shouldIndentNextWrite) {
            this.writer.write(indentation);
            this.shouldIndentNextWrite = false;
        }

        String indentedLine = line.replaceAll("\n", "\n" + indentation);

        // withhold indentation until later
        if (indentedLine.endsWith("\n" + indentation)) {
            indentedLine = indentedLine.substring(0, indentedLine.length() - indentation.length());
            shouldIndentNextWrite = true;
        }
        this.writer.write(indentedLine);
    }

    @Override
    public void flush() throws IOException {
        this.writer.flush();
    }

    @Override
    public void close() throws IOException {
        this.writer.close();
    }

    @Override
    public void write(char[] cbuf, int off, int len) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(int c) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(char[] cbuf) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(String str, int off, int len) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Writer append(CharSequence csq) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Writer append(CharSequence csq, int start, int end) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Writer append(char c) throws IOException {
        throw new UnsupportedOperationException();
    }
}
