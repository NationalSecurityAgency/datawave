package datawave.age.off.util;

import javax.xml.namespace.NamespaceContext;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

public class IndentingDelegatingXMLStreamWriter implements XMLStreamWriter {
    private final XMLStreamWriter writer;
    private String indentation;
    private boolean firstWrite = true;
    // only write pending indentation (following trailing newline) if additional calls are made to writer
    private boolean pendingIndentation;

    public IndentingDelegatingXMLStreamWriter(String indentation, XMLStreamWriter writer) {
        this.indentation = indentation;
        this.writer = writer;
    }

    @Override
    public void writeStartElement(String name) throws XMLStreamException {
        conditionallyIndent();
        this.writer.writeStartElement(name);
    }

    @Override
    public void writeStartElement(String uri, String name) throws XMLStreamException {
        conditionallyIndent();
        this.writer.writeStartElement(uri, name);
    }

    @Override
    public void writeStartElement(String prefix, String name, String uri) throws XMLStreamException {
        conditionallyIndent();
        this.writer.writeStartElement(prefix, name, uri);
    }

    @Override
    public void writeEmptyElement(String uri, String name) throws XMLStreamException {
        conditionallyIndent();
        this.writer.writeEmptyElement(uri, name);
    }

    @Override
    public void writeEmptyElement(String prefix, String uri, String name) throws XMLStreamException {
        conditionallyIndent();
        this.writer.writeEmptyElement(prefix, uri, name);
    }

    @Override
    public void writeEmptyElement(String name) throws XMLStreamException {
        conditionallyIndent();
        this.writer.writeEmptyElement(name);
    }

    @Override
    public void writeEndElement() throws XMLStreamException {
        conditionallyIndent();
        this.writer.writeEndElement();
    }

    @Override
    public void writeEndDocument() throws XMLStreamException {
        conditionallyIndent();
        this.writer.writeEndDocument();
    }

    @Override
    public void close() throws XMLStreamException {
        this.writer.close();
    }

    @Override
    public void flush() throws XMLStreamException {
        this.writer.flush();
    }

    @Override
    public void writeAttribute(String name, String value) throws XMLStreamException {
        this.writer.writeAttribute(name, value);
    }

    @Override
    public void writeAttribute(String prefix, String uri, String name, String value) throws XMLStreamException {
        this.writer.writeAttribute(prefix, uri, name, value);

    }

    @Override
    public void writeAttribute(String prefix, String name, String value) throws XMLStreamException {
        this.writer.writeAttribute(prefix, name, value);
    }

    @Override
    public void writeNamespace(String prefix, String uri) throws XMLStreamException {
        conditionallyIndent();
        this.writer.writeAttribute(prefix, uri);
    }

    @Override
    public void writeDefaultNamespace(String uri) throws XMLStreamException {
        conditionallyIndent();
        this.writer.writeDefaultNamespace(uri);
    }

    @Override
    public void writeComment(String comment) throws XMLStreamException {
        conditionallyIndent();
        this.writer.writeComment(comment);
    }

    @Override
    public void writeProcessingInstruction(String target) throws XMLStreamException {
        conditionallyIndent();
        this.writer.writeProcessingInstruction(target);

    }

    @Override
    public void writeProcessingInstruction(String target, String data) throws XMLStreamException {
        conditionallyIndent();
        this.writer.writeProcessingInstruction(target, data);
    }

    @Override
    public void writeCData(String cdata) throws XMLStreamException {
        conditionallyIndent();
        this.writer.writeCData(cdata);
    }

    @Override
    public void writeDTD(String dtd) throws XMLStreamException {
        conditionallyIndent();
        this.writer.writeDTD(dtd);
    }

    @Override
    public void writeEntityRef(String name) throws XMLStreamException {
        conditionallyIndent();
        this.writer.writeEntityRef(name);
    }

    @Override
    public void writeStartDocument() throws XMLStreamException {
        conditionallyIndent();
        this.writer.writeStartDocument();

    }

    @Override
    public void writeStartDocument(String version) throws XMLStreamException {
        conditionallyIndent();
        this.writer.writeStartDocument(version);
    }

    @Override
    public void writeStartDocument(String encoding, String version) throws XMLStreamException {
        conditionallyIndent();
        this.writer.writeStartDocument(encoding, version);
    }

    @Override
    public void writeCharacters(String text) throws XMLStreamException {
        conditionallyIndent();

        if ("\n".equals(text)) {
            this.writer.writeCharacters(text);
            this.pendingIndentation = true;
            return;
        }

        if (!text.contains("\n")) {
            this.writer.writeCharacters(text);
            return;
        }

        if (text.endsWith("\n")) {
            writeCharacters(text.substring(0, text.length() - 1));
            writeCharacters("\n");
            return;
        }

        // that any newline character is preceded by non-newline characters
        // AND followed by non-newline characters
        String[] lines = text.split("\n", -1);
        for (int i = 0; i < lines.length; i++) {
            if (i != 0) {
                this.writer.writeCharacters(indentation);
            }

            this.writer.writeCharacters(lines[i]);

            if (i != lines.length - 1) {
                this.writer.writeCharacters("\n");
                this.writer.writeCharacters(indentation);
            }
        }
    }

    @Override
    public void writeCharacters(char[] text, int start, int length) throws XMLStreamException {
        writeCharacters(new String(text, start, length));
    }

    @Override
    public String getPrefix(String uri) throws XMLStreamException {
        return this.writer.getPrefix(uri);
    }

    @Override
    public void setPrefix(String prefix, String uri) throws XMLStreamException {
        this.writer.setPrefix(prefix, uri);
    }

    @Override
    public void setDefaultNamespace(String namespace) throws XMLStreamException {
        this.writer.setDefaultNamespace(namespace);
    }

    @Override
    public void setNamespaceContext(NamespaceContext namespaceContext) throws XMLStreamException {
        this.writer.setNamespaceContext(namespaceContext);
    }

    @Override
    public NamespaceContext getNamespaceContext() {
        return this.writer.getNamespaceContext();
    }

    @Override
    public Object getProperty(String name) throws IllegalArgumentException {
        return this.writer.getProperty(name);
    }

    private void conditionallyIndent() throws XMLStreamException {
        if (firstWrite) {
            this.writer.writeCharacters(indentation);
            this.firstWrite = false;
        } else if (pendingIndentation) {
            this.writer.writeCharacters(indentation);
            this.pendingIndentation = false;
        }
    }

    public void setIndentation(String indentation) {
        this.indentation = indentation;
    }
}
