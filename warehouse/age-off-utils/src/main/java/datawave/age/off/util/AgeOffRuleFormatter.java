package datawave.age.off.util;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Formats a rule
 */
public class AgeOffRuleFormatter {
    private static final Logger log = LoggerFactory.getLogger(AgeOffRuleFormatter.class);

    private final AgeOffRuleConfiguration configuration;
    private final String indent;
    private XMLStreamWriter writer;

    public AgeOffRuleFormatter(AgeOffRuleConfiguration configuration) throws XMLStreamException {
        this.configuration = configuration;
        this.indent = this.configuration.getIndentation();
    }

    @VisibleForTesting
    void format(Writer writer) throws IOException, XMLStreamException {
        format(XMLOutputFactory.newDefaultFactory().createXMLStreamWriter(writer));
    }

    /**
     * Reformats each line from reader and outputs them to writer. Closes both reader and writer when finished.
     *
     * @throws IOException
     *             i/o exception with writer
     */
    public void format(XMLStreamWriter xmlStreamWriter) throws IOException {
        try {
            this.writer = xmlStreamWriter;
            openRuleElement();
            writeFilterClass();
            writeTtl();
            writeMergeElement();
            writeMatchPattern();
            writeCustomElements();
            closeRuleElement();
        } catch (XMLStreamException e) {
            throw new IOException(e);
        }
    }

    private void openRuleElement() throws IOException, XMLStreamException {
        writer.writeStartElement("rule");

        String ruleLabel = this.configuration.getRuleLabel();
        if (null != ruleLabel) {
            log.debug("Adding label attribute to rule: {}", ruleLabel);
            writer.writeAttribute("label", ruleLabel);
        }

        if (this.configuration.shouldMerge()) {
            log.debug("Adding merge attribute to rule");
            writer.writeAttribute("mode", "merge");
        }
        this.writer.writeCharacters("\n");
    }

    private void writeFilterClass() throws XMLStreamException {
        this.writer.writeCharacters(indent); // follows newline so it's adding indentation
        this.writer.writeStartElement("filterClass"); // also adding indentation...
        this.writer.writeCharacters(this.configuration.getFilterClass().getName());
        this.writer.writeEndElement();
        this.writer.writeCharacters("\n");
    }

    private void writeTtl() throws XMLStreamException {
        String duration = this.configuration.getTtlDuration();

        if (duration != null) {
            String units = configuration.getTtlUnits();
            log.debug("Writing ttl for duration {} and units {}", duration, units);
            this.writer.writeCharacters(indent);
            this.writer.writeStartElement("ttl");
            this.writer.writeAttribute("units", units);
            this.writer.writeCharacters(duration);
            this.writer.writeEndElement();
            this.writer.writeCharacters("\n");
        }
    }

    private void writeMergeElement() throws XMLStreamException {
        if (this.configuration.shouldMerge()) {
            log.debug("Writing ismerge element");
            this.writer.writeCharacters(indent);
            this.writer.writeStartElement("ismerge");
            this.writer.writeCharacters("true");
            this.writer.writeEndElement();
            this.writer.writeCharacters("\n");
        }
    }

    private void writeMatchPattern() throws XMLStreamException, IOException {
        if (configuration.getPatternConfiguration() == null) {
            return;
        }

        log.debug("Writing match pattern");

        this.writer.writeCharacters(indent);
        this.writer.writeStartElement("matchPattern");
        this.writer.writeCharacters("\n");
        this.writer.flush();

        AgeOffCsvToMatchPatternFormatter patternFormatter = new AgeOffCsvToMatchPatternFormatter(configuration.getPatternConfiguration());
        // add two indentations: one for items under the rule element and another for items under the matchPattern element
        patternFormatter.write(new IndentingDelegatingXMLStreamWriter(this.indent + this.indent, this.writer));

        this.writer.writeCharacters(indent);
        this.writer.writeEndElement();
        this.writer.writeCharacters("\n");
    }

    private void writeCustomElements() throws IOException, XMLStreamException {
        ArrayList<JAXBElement<?>> customElements = this.configuration.getCustomElements();

        if (null == customElements) {
            return;
        }

        for (JAXBElement<?> customElement : customElements) {
            writeCustomElement(customElement);
        }
    }

    private void writeCustomElement(JAXBElement<?> customElement) throws IOException, XMLStreamException {
        this.writer.writeCharacters(indent);
        marshalObject(customElement);
        this.writer.writeCharacters("\n");
    }

    private void marshalObject(JAXBElement<?> obj) throws IOException {
        try {
            JAXBContext jc = JAXBContext.newInstance(AnyXmlElement.class, obj.getClass());
            Marshaller marshaller = jc.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FRAGMENT, Boolean.TRUE);
            marshaller.marshal(obj, this.writer);
        } catch (JAXBException e) {
            throw new IOException(e);
        }
    }

    private void closeRuleElement() throws XMLStreamException {
        writer.writeEndElement();
        this.writer.writeCharacters("\n");
    }
}
