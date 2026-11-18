package datawave.webservice.query.result.event;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import datawave.marking.AccessExpressionMarkings;

public class SimpleFieldTest {

    @Test
    public void deprecatedMarkingsAccessorsBridgeThroughColumnVisibility() {
        SimpleField field = new SimpleField();

        // TODO remove-accumulo-utils: exercise deprecated Markings compatibility accessors until Phase 3
        assertNull(field.getMarkings());
        field.setMarkings(AccessExpressionMarkings.createMarkings("B&A"));
        assertEquals("A&B", field.getColumnVisibility());
        assertNotNull(field.getMarkings());
        assertEquals("A&B", new String(field.getMarkings().toColumnVisibility().getExpression(), StandardCharsets.UTF_8));

        field.setMarkings(null);
        assertNull(field.getColumnVisibility());
        assertNull(field.getMarkings());
    }
}
