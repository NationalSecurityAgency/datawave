package datawave.age.off.util;

import java.io.IOException;
import java.io.Writer;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Using an AgeOffFileConfiguration, writes an xml age off file containing an optional parent reference and a set of ordered rules.
 */
public class AgeOffFileGenerator {
    private static final Logger log = LoggerFactory.getLogger(AgeOffFileGenerator.class);

    private final AgeOffFileConfiguration configuration;
    private final String indent;
    private XMLStreamWriter writer;
    private IndentingDelegatingXMLStreamWriter ruleXmlStreamWriter;

    public AgeOffFileGenerator(AgeOffFileConfiguration configuration) throws XMLStreamException {
        this.configuration = configuration;
        this.indent = this.configuration.getIndentation();
    }

    public void format(Writer writer) throws XMLStreamException, IOException {
        format(XMLOutputFactory.newDefaultFactory().createXMLStreamWriter(writer));
    }

    /**
     * Writes the file to the writer.
     *
     * @throws IOException
     *             i/o exception with writer
     */
    public void format(XMLStreamWriter xmlStreamWriter) throws IOException {
        this.writer = xmlStreamWriter;
        try {
            openConfigurationElement();
            writeParentElement();
            writeRules();
            closeConfiguration();
        } catch (XMLStreamException e) {
            throw new IOException(e);
        }
    }

    private void closeConfiguration() throws IOException, XMLStreamException {
        this.writer.writeEndElement();
    }

    private void writeParentElement() throws IOException, XMLStreamException {
        String parentFileName = this.configuration.getParentFileName();

        if (null != parentFileName) {
            log.debug("Writing parent file name: {}", parentFileName);
            this.writer.writeCharacters(this.indent);
            this.writer.writeStartElement("parent");
            this.writer.writeCharacters(parentFileName);
            this.writer.writeEndElement();
            this.writer.writeCharacters("\n");
        }
    }

    private void writeRules() throws IOException, XMLStreamException {
        this.writer.writeCharacters(this.indent);
        this.writer.writeStartElement("rules");
        this.writer.writeCharacters("\n");

        for (AgeOffRuleConfiguration ruleConfiguration : this.configuration.getRuleConfigurations()) {
            writeRule(ruleConfiguration);
        }

        this.writer.writeCharacters(this.indent);
        this.writer.writeEndElement();
        this.writer.writeCharacters("\n");
    }

    private void writeRule(AgeOffRuleConfiguration ruleConfiguration) throws IOException, XMLStreamException {
        log.debug("formatting ruleConfiguration {}", ruleConfiguration.getRuleLabel());

        AgeOffRuleFormatter ruleFormatter = new AgeOffRuleFormatter(ruleConfiguration);

        // add two indentations: one for under the ageOffConfiguration element and another to go under the rules element
        String ruleIndentation = this.configuration.getIndentation() + this.configuration.getIndentation();
        if (ruleXmlStreamWriter == null) {
            ruleXmlStreamWriter = new IndentingDelegatingXMLStreamWriter(ruleIndentation, this.writer);
        } else {
            ruleXmlStreamWriter.setIndentation(ruleIndentation);
        }
        ruleFormatter.format(ruleXmlStreamWriter);

    }

    private void openConfigurationElement() throws IOException, XMLStreamException {
        this.writer.writeStartElement("ageoffConfiguration");
        this.writer.writeCharacters("\n");
    }
}
