package datawave.age.off.util;

import java.io.IOException;
import java.io.Writer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Using an AgeOffFileConfiguration, writes an xml age off file containing an optional parent reference and a set of ordered rules.
 */
public class AgeOffFileGenerator {
    private static final Logger log = LoggerFactory.getLogger(AgeOffFileGenerator.class);

    private final AgeOffFileConfiguration configuration;
    private final String indent;
    private Writer writer;

    public AgeOffFileGenerator(AgeOffFileConfiguration configuration) {
        this.configuration = configuration;
        this.indent = this.configuration.getIndentation();
    }

    /**
     * Writes the file to the writer.
     *
     * @throws IOException
     *             i/o exception with writer
     */
    public void format(Writer writer) throws IOException {
        this.writer = writer;
        openConfigurationElement();
        writeParentElement();
        writeRules();
        closeConfiguration();
    }

    private void closeConfiguration() throws IOException {
        this.writer.write("</ageoffConfiguration>\n");
    }

    private void writeParentElement() throws IOException {
        String parentFileName = this.configuration.getParentFileName();

        if (null != parentFileName) {
            log.debug("Writing parent file name: {}", parentFileName);
            this.writer.write(this.indent + "<parent>" + parentFileName + "</parent>\n");
        }
    }

    private void writeRules() throws IOException {
        this.writer.write(this.indent + "<rules>\n");

        for (AgeOffRuleConfiguration ruleConfiguration : this.configuration.getRuleConfigurations()) {
            writeRule(ruleConfiguration);
        }

        this.writer.write(this.indent + "</rules>\n");
    }

    private void writeRule(AgeOffRuleConfiguration ruleConfiguration) throws IOException {
        log.debug("formatting ruleConfiguration {}", ruleConfiguration.getRuleLabel());

        AgeOffRuleFormatter ruleFormatter = new AgeOffRuleFormatter(ruleConfiguration);

        // add two indentations: one for under the ageOffConfiguration element and another to go under the rules element
        String ruleIndentation = this.configuration.getIndentation() + this.configuration.getIndentation();
        ruleFormatter.format(new IndentingDelegatingWriter(ruleIndentation, this.writer));

    }

    private void openConfigurationElement() throws IOException {
        this.writer.write("<?xml version=\"1.0\"?>\n");
        this.writer.write("<ageoffConfiguration>\n");
    }
}
