package datawave.ingest.data.config;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import datawave.ingest.data.Type;
import datawave.marking.AccessExpressionMarkings;
import datawave.marking.Markings;

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
        Assert.assertNull(markingsHelper.getFieldMarking(FIELD_NAME));
    }

    @Test
    public void honorSpecifiedDatatypeFieldMarkings() {
        // use for both a field marking Configuration and creating a MarkingsHelper
        String typeName = "sameDataType";

        // use the type name to define a field marking in the Configuration
        Configuration conf = createConfWithFieldMarking(typeName);

        // use the same type name to retrieve a markings helper and retrieve the field marking
        MarkingsHelper markingsHelper = new MarkingsHelper.NoOp(conf, createType(typeName));
        Markings<?> fieldMarkings = markingsHelper.getFieldMarking(FIELD_NAME);

        Assert.assertNotNull(fieldMarkings);
        Assert.assertEquals(FIELD_MARKING_VALUE, ((AccessExpressionMarkings) fieldMarkings).getAccessExpression().getExpression());
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
