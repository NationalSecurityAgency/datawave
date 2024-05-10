package datawave.age.off.util;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.List;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.xerces.dom.DocumentImpl;
import org.junit.Test;
import org.w3c.dom.Element;

import datawave.ingest.util.cache.watch.AgeOffRuleLoader;

public class RuleConfigDocumentTest {

    @Test
    public void includesFilterClass() throws IOException {
        String actual = transformToXmlString(new AgeOffRuleLoader.RuleConfig("myclass", 1));

        // @formatter:off
        String expected = "<rule>\n" +
                "  <filterClass>myclass</filterClass>\n" +
                "</rule>\n";
        // @formatter:on
        assertEquals(actual, expected, actual);
    }

    @Test
    public void includesTtl() throws IOException {
        AgeOffRuleLoader.RuleConfig ruleConfig = new AgeOffRuleLoader.RuleConfig("myclass", 1);
        ruleConfig.ttlUnits("h");
        ruleConfig.ttlValue("2468");
        String actual = transformToXmlString(ruleConfig);
        // @formatter:off
        String expected = "<rule>\n" +
                "  <filterClass>myclass</filterClass>\n" +
                "  <ttl units=\"h\">2468</ttl>\n" +
                "</rule>\n";
        // @formatter:on
        assertEquals(actual, expected, actual);
    }

    @Test
    public void includesMatchPattern() throws IOException {
        AgeOffRuleLoader.RuleConfig ruleConfig = new AgeOffRuleLoader.RuleConfig("myclass", 1);
        ruleConfig.matchPattern("1234\n");
        String actual = transformToXmlString(ruleConfig);

        // @formatter:off
        String expected = "<rule>\n" +
                "  <filterClass>myclass</filterClass>\n" +
                "  <matchPattern>\n1234\n</matchPattern>\n" +
                "</rule>\n";
        // @formatter:on
        assertEquals(actual, expected, actual);
    }

    @Test
    public void includesMerge() throws IOException {
        AgeOffRuleLoader.RuleConfig ruleConfig = new AgeOffRuleLoader.RuleConfig("myclass", 1);
        ruleConfig.setIsMerge(true);
        String actual = transformToXmlString(ruleConfig);

        // @formatter:off
        String expected = "<rule mode=\"merge\">\n" +
                "  <filterClass>myclass</filterClass>\n" +
                "  <ismerge>true</ismerge>\n" +
                "</rule>\n";
        // @formatter:on
        assertEquals(actual, expected, actual);
    }

    @Test
    public void includesCustomElements() throws IOException {
        AgeOffRuleLoader.RuleConfig ruleConfig = new AgeOffRuleLoader.RuleConfig("myclass", 1);
        List<Element> elements = Arrays.asList(new DocumentImpl().createElement("a"), new DocumentImpl().createElement("b"));
        ruleConfig.customElements(elements);
        String actual = transformToXmlString(ruleConfig);

        // @formatter:off
        String expected = "<rule>\n" +
                "  <filterClass>myclass</filterClass>\n" +
                "  <a/>\n" +
                "  <b/>\n" +
                "</rule>\n";
        // @formatter:on
        assertEquals(actual, expected, actual);
    }

    @Test
    public void includesLabel() throws IOException {
        AgeOffRuleLoader.RuleConfig ruleConfig = new AgeOffRuleLoader.RuleConfig("myclass", 1);
        ruleConfig.label("tag");
        String actual = transformToXmlString(ruleConfig);

        // @formatter:off
        String expected = "<rule label=\"tag\">\n" +
                "  <filterClass>myclass</filterClass>\n" +
                "</rule>\n";
        // @formatter:on
        assertEquals(actual, expected, actual);
    }

    @Test
    public void includesAll() throws IOException {
        AgeOffRuleLoader.RuleConfig ruleConfig = new AgeOffRuleLoader.RuleConfig("myclass", 1);
        ruleConfig.ttlUnits("h");
        ruleConfig.ttlValue("2468");
        ruleConfig.matchPattern("1234\n");
        ruleConfig.setIsMerge(true);
        List<Element> elements = Arrays.asList(new DocumentImpl().createElement("a"), new DocumentImpl().createElement("b"));
        ruleConfig.customElements(elements);
        ruleConfig.label("tag");
        String actual = transformToXmlString(ruleConfig);

        // @formatter:off
        String expected = "<rule label=\"tag\" mode=\"merge\">\n" +
                "  <filterClass>myclass</filterClass>\n" +
                "  <ismerge>true</ismerge>\n" +
                "  <ttl units=\"h\">2468</ttl>\n" +
                "  <matchPattern>\n" +
                "1234\n" +
                "</matchPattern>\n" +
                "  <a/>\n" +
                "  <b/>\n" +
                "</rule>\n";
        // @formatter:on
        assertEquals(actual, expected, actual);
    }

    private String transformToXmlString(AgeOffRuleLoader.RuleConfig ruleConfig) throws IOException {
        try {
            Transformer transformer = initializeXmlTransformer();

            Writer writer = new StringWriter();

            StreamResult result = new StreamResult(writer);
            DOMSource source = new DOMSource(new RuleConfigDocument(ruleConfig));
            transformer.transform(source, result);

            return writer.toString();
        } catch (TransformerException e) {
            throw new IOException("Failed to transform to XML", e);
        }
    }

    private static Transformer initializeXmlTransformer() throws TransformerConfigurationException {
        Transformer trans = TransformerFactory.newInstance().newTransformer();
        trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        trans.setOutputProperty(OutputKeys.METHOD, "xml");
        trans.setOutputProperty(OutputKeys.INDENT, "yes");
        trans.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
        return trans;
    }
}
