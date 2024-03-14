package datawave.age.off.util;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Formats a rule
 */
public class AgeOffRuleFormatter {
    private static final Logger log = LoggerFactory.getLogger(AgeOffRuleFormatter.class);

    private final AgeOffRuleConfiguration configuration;
    private final String indent;
    private final Writer writer;

    public AgeOffRuleFormatter(AgeOffRuleConfiguration configuration) {
        this.configuration = configuration;
        this.indent = this.configuration.getIndentation();
        this.writer = this.configuration.getWriter();
    }

    /**
     * Reformats each line from reader and outputs them to writer. Closes both reader and writer when finished.
     *
     * @throws IOException
     *             i/o exception with writer
     */
    public void format() throws IOException {
        openRuleElement();
        writeFilterClass();
        writeTtl();
        writeMergeElement();
        writeMatchPattern();
        writeCustomElements();
        closeRuleElement();
    }

    private void openRuleElement() throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("<rule");

        String ruleLabel = this.configuration.getRuleLabel();
        if (null != ruleLabel) {
            log.debug("Adding label attribute to rule: {}", ruleLabel);
            sb.append(" label=\"").append(ruleLabel).append("\"");
        }

        if (this.configuration.shouldMerge()) {
            log.debug("Adding merge attribute to rule");
            sb.append(" mode=\"merge\"");
        }
        sb.append(">\n");

        this.writer.write(sb.toString());
    }

    private void writeFilterClass() throws IOException {
        this.writer.write(indent + "<filterClass>" + this.configuration.getFilterClass().getName() + "</filterClass>\n");
    }

    private void writeTtl() throws IOException {
        String duration = this.configuration.getTtlDuration();

        if (duration != null) {
            String units = configuration.getTtlUnits();
            log.debug("Writing ttl for duration {} and units {}", duration, units);
            this.writer.write(indent + "<ttl units=\"" + units + "\">" + duration + "</ttl>\n");
        }
    }

    private void writeMergeElement() throws IOException {
        if (this.configuration.shouldMerge()) {
            log.debug("Writing ismerge element");
            this.writer.write(configuration.getIndentation() + "<ismerge>true</ismerge>\n");
        }
    }

    private void writeMatchPattern() throws IOException {
        if (configuration.getPatternConfiguration() == null) {
            return;
        }

        log.debug("Writing match pattern");

        this.writer.write(indent + "<matchPattern>\n");

        AgeOffCsvToMatchPatternFormatter patternFormatter = new AgeOffCsvToMatchPatternFormatter(configuration.getPatternConfiguration());
        patternFormatter.write();

        this.writer.write(indent + "</matchPattern>\n");
    }

    private void writeCustomElements() throws IOException {
        ArrayList<JAXBElement<?>> customElements = this.configuration.getCustomElements();

        if (null == customElements) {
            return;
        }

        for (JAXBElement<?> customElement : customElements) {
            writeCustomElement(customElement);
        }
    }

    private void writeCustomElement(JAXBElement<?> customElement) throws IOException {
        StringWriter writer = new StringWriter();
        marshalObject(customElement, writer);
        log.debug("Marshalled custom object to String: {}", writer);
        this.writer.write(indent + writer.toString() + "\n");
    }

    private void marshalObject(JAXBElement<?> obj, Writer writer) throws IOException {
        try {
            JAXBContext jc = JAXBContext.newInstance(AnyXmlElement.class, obj.getClass());
            Marshaller marshaller = jc.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FRAGMENT, Boolean.TRUE);
            marshaller.marshal(obj, writer);
        } catch (JAXBException e) {
            throw new IOException(e);
        }
    }

    private void closeRuleElement() throws IOException {
        this.writer.write("</rule>\n");
    }
}
