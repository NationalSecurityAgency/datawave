package datawave.ingest.data.config;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import datawave.ingest.data.Type;

public class MarkingsHelperTest {
    public static final String FIELD_NAME = "FIELDNAME1";
    public static final String FIELD_MARKING_VALUE = "HIPPA";

    @Test
    public void ignoreOtherDatatypeFieldMarkings() {
        // use a short type name to define a field marking in the Configuration
        Configuration conf = createConfWithFieldMarking("shortDataType");

        // use a different (lengthy) type name to retrieve a markings helper
        Type typeWithLongName = createType("veryLongDataTypeCompletelyUnrelated");
        MarkingsHelper markingsHelper = new MarkingsHelper.NoOp(conf, typeWithLongName);

        // the lengthy type name has no associated field marking. This previously threw an exception
        assertNull(markingsHelper.getFieldVisibility(FIELD_NAME));
    }

    @Test
    public void honorSpecifiedDatatypeFieldMarkings() {
        // use for both a field marking Configuration and creating a MarkingsHelper
        String typeName = "sameDataType";

        // use the type name to define a field marking in the Configuration
        Configuration conf = createConfWithFieldMarking(typeName);

        // use the same type name to retrieve a markings helper and retrieve the field marking
        MarkingsHelper markingsHelper = new MarkingsHelper.NoOp(conf, createType(typeName));
        ColumnVisibility fieldVisibility = markingsHelper.getFieldVisibility(FIELD_NAME);

        assertNotNull(fieldVisibility);
        assertEquals(FIELD_MARKING_VALUE, new String(fieldVisibility.getExpression(), UTF_8));
    }

    @Test
    public void defaultVisibility() {
        String typeName = "defaultVisDataType";
        Configuration conf = new Configuration();
        conf.set(typeName + MarkingsHelper.DEFAULT_MARKING, "PUBLIC|PRIVATE");

        MarkingsHelper markingsHelper = new MarkingsHelper.NoOp(conf, createType(typeName));
        assertEquals(new ColumnVisibility("PUBLIC|PRIVATE"), markingsHelper.getDefaultVisibility());

        MarkingsHelper noDefault = new MarkingsHelper.NoOp(new Configuration(), createType(typeName));
        assertNull(noDefault.getDefaultVisibility());
    }

    @Test
    public void markFieldUsesFieldThenDefaultVisibility() {
        String typeName = "markFieldDataType";
        Configuration conf = createConfWithFieldMarking(typeName);
        conf.set(typeName + MarkingsHelper.DEFAULT_MARKING, "PUBLIC");
        MarkingsHelper markingsHelper = new MarkingsHelper.NoOp(conf, createType(typeName));

        // a field with a configured override gets the override
        NormalizedFieldAndValue overridden = new NormalizedFieldAndValue(FIELD_NAME, "value");
        markingsHelper.markField(overridden);
        assertEquals(new ColumnVisibility(FIELD_MARKING_VALUE), overridden.getVisibility());

        // a field without an override gets the default
        NormalizedFieldAndValue other = new NormalizedFieldAndValue("OTHER_FIELD", "value");
        markingsHelper.markField(other);
        assertEquals(new ColumnVisibility("PUBLIC"), other.getVisibility());

        // a field that already has a visibility keeps it
        NormalizedFieldAndValue alreadySet = new NormalizedFieldAndValue(FIELD_NAME, "value", new ColumnVisibility("A&B"));
        markingsHelper.markField(alreadySet);
        assertEquals(new ColumnVisibility("A&B"), alreadySet.getVisibility());

        // without any configured visibility the field visibility stays null (meaning the event visibility is used)
        MarkingsHelper unconfigured = new MarkingsHelper.NoOp(new Configuration(), createType(typeName));
        NormalizedFieldAndValue unmarked = new NormalizedFieldAndValue("OTHER_FIELD", "value");
        unconfigured.markField(unmarked);
        assertNull(unmarked.getVisibility());
    }

    private Configuration createConfWithFieldMarking(String typeName) {
        Configuration conf = new Configuration();
        conf.set(typeName + "." + FIELD_NAME + MarkingsHelper.FIELD_MARKING, FIELD_MARKING_VALUE);
        return conf;
    }

    private Type createType(String typeName) {
        return new Type(typeName, "outputName", null, null, null, 0, null);
    }
}
