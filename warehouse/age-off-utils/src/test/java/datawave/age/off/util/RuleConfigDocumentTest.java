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

import datawave.ingest.util.cache.watch.RuleConfig;

public class RuleConfigDocumentTest {

    @Test
    public void includesFilterClass() throws IOException {
        String actual = transformToXmlString(new RuleConfig.Builder("myclass", 1).build());

        // @formatter:off
        String expected = "<rule>\n" +
                "  <filterClass>myclass</filterClass>\n" +
                "</rule>\n";
        // @formatter:on
        assertEquals(actual, expected, actual);
    }

    @Test
    public void includesTtl() throws IOException {
        // @formatter:off
        RuleConfig.Builder builder = new RuleConfig.Builder("myclass", 1)
                .setTtlUnits("h")
                .setTtlValue("2468");
        String actual = transformToXmlString(builder.build());

        String expected = "<rule>\n" +
                "  <filterClass>myclass</filterClass>\n" +
                "  <ttl units=\"h\">2468</ttl>\n" +
                "</rule>\n";
        // @formatter:on
        assertEquals(actual, expected, actual);
    }

    @Test
    public void includesMatchPattern() throws IOException {
        // @formatter:off
        RuleConfig.Builder builder = new RuleConfig.Builder("myclass", 1)
                .setMatchPattern("1234\n");
        String actual = transformToXmlString(builder.build());

        String expected = "<rule>\n" +
                "  <filterClass>myclass</filterClass>\n" +
                "  <matchPattern>\n1234\n</matchPattern>\n" +
                "</rule>\n";
        // @formatter:on
        assertEquals(actual, expected, actual);
    }

    @Test
    public void includesMerge() throws IOException {
        // @formatter:off
        RuleConfig.Builder builder = new RuleConfig.Builder("myclass", 1)
                .setIsMerge(true);
        String actual = transformToXmlString(builder.build());

        String expected = "<rule mode=\"merge\">\n" +
                "  <filterClass>myclass</filterClass>\n" +
                "</rule>\n";
        // @formatter:on
        assertEquals(actual, expected, actual);
    }

    @Test
    public void includesCustomElements() throws IOException {
        List<Element> elements = Arrays.asList(new DocumentImpl().createElement("a"), new DocumentImpl().createElement("b"));

        // @formatter:off
        RuleConfig.Builder builder = new RuleConfig.Builder("myclass", 1)
                .setCustomElements(elements);
        String actual = transformToXmlString(builder.build());

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
        // @formatter:off
        RuleConfig.Builder builder = new RuleConfig.Builder("myclass", 1)
                .setLabel("tag");
        String actual = transformToXmlString(builder.build());

        String expected = "<rule label=\"tag\">\n" +
                "  <filterClass>myclass</filterClass>\n" +
                "</rule>\n";
        // @formatter:on
        assertEquals(actual, expected, actual);
    }

    @Test
    public void includesAll() throws IOException {
        List<Element> elements = Arrays.asList(new DocumentImpl().createElement("a"), new DocumentImpl().createElement("b"));

        // @formatter:off
        RuleConfig.Builder builder = new RuleConfig.Builder("myclass", 1)
                .setTtlUnits("h")
                .setTtlValue("2468")
                .setMatchPattern("1234\n")
                .setIsMerge(true)
                .setCustomElements(elements)
                .setLabel("tag");
        String actual = transformToXmlString(builder.build());

        String expected = "<rule label=\"tag\" mode=\"merge\">\n" +
                "  <filterClass>myclass</filterClass>\n" +
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

    private String transformToXmlString(RuleConfig ruleConfig) throws IOException {
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
