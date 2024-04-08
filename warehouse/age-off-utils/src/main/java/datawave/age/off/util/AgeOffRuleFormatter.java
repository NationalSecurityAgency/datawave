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

import com.google.common.annotations.VisibleForTesting;

/**
 * Formats a rule
 */
public class AgeOffRuleFormatter {
    private static final Logger log = LoggerFactory.getLogger(AgeOffRuleFormatter.class);

    private final AgeOffRuleConfiguration configuration;
    private final String indent;
    private Writer writer;

    public AgeOffRuleFormatter(AgeOffRuleConfiguration configuration) {
        this.configuration = configuration;
        this.indent = this.configuration.getIndentation();
    }

    /**
     * Outputs the configured rule to the writer
     *
     * @param writer
     *            output writer
     * @throws IOException
     *             i/o exception with writer
     */
    @VisibleForTesting
    void format(Writer writer) throws IOException {
        this.writer = writer;
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

        // add two indentations: one for items under the rule element and another for items under the matchPattern element
        String extraIndentation = this.indent + this.indent;
        patternFormatter.write(new IndentingDelegatingWriter(extraIndentation, this.writer));

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
        StringWriter tempWriter = new StringWriter();
        marshalObject(customElement, tempWriter);
        log.debug("Marshalled custom object to String: {}", tempWriter);
        this.writer.write(indent + tempWriter + "\n");
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
