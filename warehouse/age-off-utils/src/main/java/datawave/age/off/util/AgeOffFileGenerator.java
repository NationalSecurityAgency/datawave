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
    private final Writer writer;

    public AgeOffFileGenerator(AgeOffFileConfiguration configuration) {
        this.configuration = configuration;
        this.indent = this.configuration.getIndentation();
        this.writer = this.configuration.getWriter();
    }

    /**
     * Writes the file to the writer.
     *
     * @throws IOException
     *             i/o exception with writer
     */
    public void format() throws IOException {
        openConfigurationElement();
        writerParentElement();
        writerRules();
        closeConfiguration();
    }

    private void closeConfiguration() throws IOException {
        this.writer.write("</ageoffConfiguration>\n");
    }

    private void writerParentElement() throws IOException {
        String parentFileName = this.configuration.getParentFileName();

        if (null != parentFileName) {
            log.debug("Writing parent file name: {}", parentFileName);
            this.writer.write(this.indent + "<parent>" + parentFileName + "</parent>\n");
        }
    }

    private void writerRules() throws IOException {
        this.writer.write(this.indent + "<rules>\n");

        for (AgeOffRuleConfiguration ruleConfiguration : this.configuration.getRuleConfigurations()) {
            writeRule(ruleConfiguration);
        }

        this.writer.write(this.indent + "</rules>\n");
    }

    private void writeRule(AgeOffRuleConfiguration ruleConfiguration) throws IOException {
        log.debug("formatting ruleConfiguration {}", ruleConfiguration.getRuleLabel());
        new AgeOffRuleFormatter(ruleConfiguration).format();
    }

    private void openConfigurationElement() throws IOException {
        this.writer.write("<ageoffConfiguration>\n");
    }
}
