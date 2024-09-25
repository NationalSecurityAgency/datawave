package datawave.age.off.util;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

import org.junit.Test;

public class IndentingDelegatingWriterTest {
    @Test
    public void addsIndentation() throws IOException {
        StringWriter originalWriter = new StringWriter();
        Writer writer = new IndentingDelegatingWriter("  ", originalWriter);
        writer.write("\n");
        writer.write("a\n");
        writer.write("b\n");
        writer.close();

        // @formatter:off
        String expectedText =
                "  \n" +
                "  a\n" +
                "  b\n";
        // @formatter:on

        assertEquals(expectedText, originalWriter.toString());
    }

    @Test
    public void nestedIndentation() throws IOException {
        Writer writer = new StringWriter();
        // outer element
        Writer outerLayer = new IndentingDelegatingWriter("--", writer);
        // nested element
        Writer innerLayer = new IndentingDelegatingWriter("  ", outerLayer);

        outerLayer.write("outer\n");

        innerLayer.write("\n");
        innerLayer.write("a\n");
        innerLayer.write("b\n");

        outerLayer.write("outer\n");

        innerLayer.close();
        // @formatter:off
        String expectedText =
                "--outer\n" +
                "--  \n" +
                "--  a\n" +
                "--  b\n" +
                "--outer\n";

        // @formatter:on
        assertEquals(expectedText, writer.toString());
    }

    @Test
    public void multiNestedIndentation() throws IOException {
        Writer writer = new StringWriter();
        // outer element
        Writer outerLayer = new IndentingDelegatingWriter("--", writer);
        // nested element
        Writer middleLayer = new IndentingDelegatingWriter("++", outerLayer);
        // further nested element
        Writer innerLayer = new IndentingDelegatingWriter("  ", middleLayer);

        outerLayer.write("outer\n");

        middleLayer.write("\n");
        middleLayer.write("a\n");
        innerLayer.write("1\n");
        innerLayer.write("2\n");
        innerLayer.write("3\n");

        middleLayer.write("b\n");

        outerLayer.write("outer\n");

        innerLayer.close();
        // @formatter:off
        String expectedText =
                "--outer\n" +
                "--++\n" +
                "--++a\n" +
                "--++  1\n" +
                "--++  2\n" +
                "--++  3\n" +
                "--++b\n" +
                "--outer\n";

        // @formatter:on
        assertEquals(expectedText, writer.toString());
    }
}
