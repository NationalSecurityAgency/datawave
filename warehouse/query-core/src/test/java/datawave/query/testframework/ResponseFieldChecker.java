package datawave.query.testframework;

import java.util.Set;

import org.junit.Assert;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;

/**
 * Logic checker for the return fields in a {@link Document}.
 */
public class ResponseFieldChecker implements QueryLogicTestHarness.DocumentChecker {

    private final Set<String> fields;
    private final Set<String> missing;

    /**
     * @param respFields
     *            list of fields that should be in the response document
     * @param missingFields
     *            fields that should not be included in the response
     */
    public ResponseFieldChecker(final Set<String> respFields, final Set<String> missingFields) {
        this.fields = respFields;
        this.missing = missingFields;
    }

    /**
     * Verifies the query response document contains all of the return fields.
     *
     * @param doc
     *            query response document
     */
    @Override
    public void assertValid(final Document doc) {
        for (final String field : this.fields) {
            final Attribute val = doc.get(field);
            Assert.assertNotNull("missing return field(" + field + ") from " + doc, val);
            if (val instanceof Attributes) {
                Attributes multiAttr = (Attributes) val;
                for (Attribute attr : multiAttr.getAttributes()) {
                    Assert.assertNotNull("missing metadata for field(" + field + ") from " + doc, attr.getMetadata());
                }
            } else {
                Assert.assertNotNull("missing metadata for field(" + field + ") from " + doc, val.getMetadata());
            }
        }

        for (final String field : this.missing) {
            final Attribute val = doc.get(field);
            Assert.assertNull("blacklisted return field(" + field + ") from " + doc, val);
        }
    }
}
