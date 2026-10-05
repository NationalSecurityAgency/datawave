package datawave.table.constants;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class MetadataColumnFamilyConstantsTest {

    @Test
    public void verifyNoConstantsChanged() {
        assertConstant(MetadataColumnFamilyConstants.COLF_E, "e");
        assertConstant(MetadataColumnFamilyConstants.COLF_EXP, "exp");
        assertConstant(MetadataColumnFamilyConstants.COLF_CONTENT, "content");
        assertConstant(MetadataColumnFamilyConstants.COLF_I, "i");
        assertConstant(MetadataColumnFamilyConstants.COLF_RI, "ri");
        assertConstant(MetadataColumnFamilyConstants.COLF_F, "f");
        assertConstant(MetadataColumnFamilyConstants.COLF_TF, "tf");
        assertConstant(MetadataColumnFamilyConstants.COLF_N, "n");
        assertConstant(MetadataColumnFamilyConstants.COLF_T, "t");
        assertConstant(MetadataColumnFamilyConstants.COLF_DESC, "desc");
        assertConstant(MetadataColumnFamilyConstants.COLF_EDGE, "edge");
        assertConstant(MetadataColumnFamilyConstants.COLF_H, "h");
        assertConstant(MetadataColumnFamilyConstants.COLF_CI, "ci");
        assertConstant(MetadataColumnFamilyConstants.COLF_CITD, "citd");
        assertConstant(MetadataColumnFamilyConstants.COLF_CISEP, "cisep");
        assertConstant(MetadataColumnFamilyConstants.COLF_COUNT, "count");
        assertConstant(MetadataColumnFamilyConstants.COLF_VERSION, "version");
        assertConstant(MetadataColumnFamilyConstants.COLF_VI, "vi");
        assertConstant(MetadataColumnFamilyConstants.COLF_WCD, "wcd");
    }

    /**
     * Verify the constant has not changed
     *
     * @param actual
     *            the actual column family constant
     * @param expected
     *            the expected constant
     */
    private void assertConstant(Text actual, String expected) {
        assertEquals(new Text(expected), actual, "Did not expect constant to change: " + actual);
    }

}
