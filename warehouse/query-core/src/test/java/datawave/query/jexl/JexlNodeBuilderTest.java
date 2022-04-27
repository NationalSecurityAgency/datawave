package datawave.query.jexl;

import datawave.test.JexlNodeAssert;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.JexlNode;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class JexlNodeBuilderTest {
    
    /**
     * Verify that when null children are not allowed when building, that an exception is thrown if the builder is given a null child.
     */
    @Test
    public void testNullChildWhenThrowOnNullChildIsTrue() {
        Assertions.assertThatIllegalArgumentException().isThrownBy(() -> JexlNodeBuilder.ASTIdentifier().throwOnNullChild(true).withChild((JexlNode) null))
                        .withMessage("Child must not be null");
    }
    
    /**
     * Verify that when null children are allowed when building, that an exception is not thrown when building, but also that the resulting node does not have
     * the null child.
     */
    @Test
    public void testNullChildWhenThrowOnNullChildrenIsFalse() {
        ASTIdentifier node = JexlNodeBuilder.ASTIdentifier().throwOnNullChild(false).withChild((JexlNode) null).build();
        JexlNodeAssert.assertThat(node).hasNumChildren(0);
    }
}
