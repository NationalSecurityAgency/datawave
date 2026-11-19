package datawave.ingest.mapreduce.handler.edge;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.mapreduce.handler.edge.define.EdgeDataBundle;
import datawave.ingest.mapreduce.handler.edge.define.EdgeDefinition;

/**
 * Verifies how vertex field visibilities are combined into the edge visibility, and the visibility bytes written for edges.
 */
public class EdgeVisibilityTest {

    private static final String EVENT_VISIBILITY = "EVENT&(PUBLIC|PRIVATE)";

    private static class VisibilityExposingHandler extends ProtobufEdgeDataTypeHandler<Text,Text,Text> {
        Text visibility(ColumnVisibility fieldVisibility, RawRecordContainer event) {
            return getVisibility(fieldVisibility, event);
        }

        Text visibility(EdgeDataBundle bundle) {
            return getVisibility(bundle);
        }
    }

    private final VisibilityExposingHandler handler = new VisibilityExposingHandler();

    private static RawRecordContainerImpl event() {
        RawRecordContainerImpl event = new RawRecordContainerImpl();
        event.setTimestamp(1749254400000L);
        event.setVisibility(new ColumnVisibility(EVENT_VISIBILITY));
        return event;
    }

    private static EdgeDataBundle bundle(ColumnVisibility sourceVisibility, ColumnVisibility sinkVisibility) {
        EdgeDefinition definition = new EdgeDefinition();
        definition.setEdgeType("TEST");
        return new EdgeDataBundle(definition, new NormalizedFieldAndValue("SOURCE", "source", sourceVisibility),
                        new NormalizedFieldAndValue("SINK", "sink", sinkVisibility), event(), null);
    }

    @Test
    public void edgeWithoutVertexVisibilitiesUsesEventVisibility() {
        EdgeDataBundle bundle = bundle(null, null);
        assertNull(bundle.getVisibility());
        assertArrayEquals(new ColumnVisibility(EVENT_VISIBILITY).flatten(), handler.visibility(bundle).copyBytes());
    }

    @Test
    public void edgeWithOneVertexVisibilityKeepsIt() {
        assertEquals(new ColumnVisibility("B&A"), bundle(new ColumnVisibility("B&A"), null).getVisibility());
        assertEquals(new ColumnVisibility("C|D"), bundle(null, new ColumnVisibility("C|D")).getVisibility());
    }

    @Test
    public void edgeCombinesVertexVisibilities() {
        EdgeDataBundle bundle = bundle(new ColumnVisibility("B&A"), new ColumnVisibility("D|C"));
        assertEquals(new ColumnVisibility("A&B&(C|D)"), bundle.getVisibility());
        assertEquals("A&B&(C|D)", handler.visibility(bundle).toString());
    }

    @Test
    public void fieldVisibilityBytesAreNormalizedExpression() {
        assertEquals("A&B", new String(handler.visibility(new ColumnVisibility("B&A"), event()).copyBytes(), UTF_8));
        assertEquals("(B&C)|(Y&Z)", new String(handler.visibility(new ColumnVisibility("(Z&Y)|(C&B)"), event()).copyBytes(), UTF_8));
    }

    @Test
    public void missingOrEmptyFieldVisibilityUsesEventVisibility() {
        byte[] expected = new ColumnVisibility(EVENT_VISIBILITY).flatten();
        assertArrayEquals(expected, handler.visibility(null, event()).copyBytes());
        assertArrayEquals(expected, handler.visibility(new ColumnVisibility(), event()).copyBytes());
    }
}
