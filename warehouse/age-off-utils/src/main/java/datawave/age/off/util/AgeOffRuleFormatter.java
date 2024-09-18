package datawave.age.off.util;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import datawave.ingest.util.cache.watch.AgeOffRuleLoader;

/**
 * Formats a rule
 */
public class AgeOffRuleFormatter {
    private static final Logger log = LoggerFactory.getLogger(AgeOffRuleFormatter.class);

    private final AgeOffRuleConfiguration configuration;
    private final String indent;
    private static int index = 0;

    public AgeOffRuleFormatter(AgeOffRuleConfiguration configuration) {
        this.configuration = configuration;
        this.indent = this.configuration.getIndentation();
    }

    /**
     * Outputs the configured rule to the writer. Will not close the writer.
     *
     * @param writer
     *            output writer
     * @throws IOException
     *             i/o exception with writer
     */
    @VisibleForTesting
    void format(Writer writer) throws IOException {

        AgeOffRuleLoader.RuleConfig ruleConfig = createRuleConfig(this.configuration);

        writer.write(transformToXmlString(ruleConfig));
    }

    private AgeOffRuleLoader.RuleConfig createRuleConfig(AgeOffRuleConfiguration configuration) throws IOException {
        AgeOffRuleLoader.RuleConfig ruleConfig = new AgeOffRuleLoader.RuleConfig(this.configuration.getFilterClass().getName(), index++);
        ruleConfig.label(configuration.getRuleLabel());
        ruleConfig.setIsMerge(this.configuration.shouldMerge());
        ruleConfig.ttlValue(this.configuration.getTtlDuration());
        ruleConfig.ttlUnits(this.configuration.getTtlUnits());
        ruleConfig.matchPattern(buildMatchPattern());
        ruleConfig.customElements(this.configuration.getCustomElements());
        return ruleConfig;
    }

    private String transformToXmlString(AgeOffRuleLoader.RuleConfig ruleConfig) throws IOException {
        try {
            Transformer trans = initializeXmlTransformer();

            Writer writer = new StringWriter();

            StreamResult result = new StreamResult(writer);
            DOMSource source = new DOMSource(new RuleConfigDocument(ruleConfig));
            trans.transform(source, result);

            return writer.toString();
        } catch (TransformerException e) {
            throw new IOException("Failed to transform to XML", e);
        }
    }

    private Transformer initializeXmlTransformer() throws TransformerConfigurationException {
        Transformer trans = TransformerFactory.newInstance().newTransformer();
        trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        trans.setOutputProperty(OutputKeys.METHOD, "xml");
        trans.setOutputProperty(OutputKeys.INDENT, "yes");
        trans.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", calculateIndentAmount());
        return trans;
    }

    private String calculateIndentAmount() {
        int length = configuration.getIndentation().length();
        // add another four for every tab
        length += (int) (4 * configuration.getIndentation().chars().filter(character -> character == '\t').count());
        return Integer.toString(length);
    }

    private String buildMatchPattern() throws IOException {
        if (configuration.getPatternConfiguration() == null) {
            return "";
        }

        log.debug("Writing match pattern");

        StringWriter writer = new StringWriter();
        AgeOffCsvToMatchPatternFormatter patternFormatter = new AgeOffCsvToMatchPatternFormatter(configuration.getPatternConfiguration());

        // add two indentations: one for items under the rule element and another for items under the matchPattern element
        String extraIndentation = this.indent + this.indent;
        patternFormatter.write(new IndentingDelegatingWriter(extraIndentation, writer));

        String result = writer.toString();

        // final indentation to precede the closing of matchPattern
        if (result.endsWith("\n")) {
            return result + this.indent;
        }
        return result;
    }
}
