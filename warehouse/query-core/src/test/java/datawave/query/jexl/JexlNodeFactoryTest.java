package datawave.query.jexl;

import datawave.test.JexlNodeAssert;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertThrows;

@RunWith(Enclosed.class)
public class JexlNodeFactoryTest {
    
    /**
     * Tests for {@link JexlNodeFactory#createAssignment(String, String)} and {@link JexlNodeFactory#createAssignment(String, boolean)}.
     */
    public static class CreateAssignmentTests {
    
        /**
         * Verify that both methods throw an exception when given a null or blank field name.
         */
        @Test
        public void testNullFieldName() {
            assertThrows("field name must not be blank", IllegalArgumentException.class, () -> JexlNodeFactory.createAssignment(null, true));
            assertThrows("field name must not be blank", IllegalArgumentException.class, () -> JexlNodeFactory.createAssignment("", true));
            assertThrows("field name must not be blank", IllegalArgumentException.class, () -> JexlNodeFactory.createAssignment(" ", true));
            assertThrows("field name must not be blank", IllegalArgumentException.class, () -> JexlNodeFactory.createAssignment(null, "string"));
            assertThrows("field name must not be blank", IllegalArgumentException.class, () -> JexlNodeFactory.createAssignment("", "string"));
            assertThrows("field name must not be blank", IllegalArgumentException.class, () -> JexlNodeFactory.createAssignment(" ", "string"));
        }
        
        /**
         * Verify that {@link JexlNodeFactory#createAssignment(String, boolean)} returns a valid assignment node for the boolean value true.
         */
        @Test
        public void testBooleanValueOfTrue() {
            ASTAssignment node = JexlNodeFactory.createAssignment("FOO", true);
            JexlNodeAssert nodeAssert = JexlNodeAssert.assertThat(node)
                            .isInstanceOf(ASTAssignment.class)
                            .hasNumChildren(2)
                            .hasValidLineage();
            
            nodeAssert.child(0)
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .child(0)
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO");
            
            nodeAssert.child(1)
                            .isInstanceOf(ASTTrueNode.class);
        }
    
        /**
         * Verify that {@link JexlNodeFactory#createAssignment(String, boolean)} returns a valid assignment node for the boolean value false.
         */
        @Test
        public void testBooleanValueOfFalse() {
            ASTAssignment node = JexlNodeFactory.createAssignment("FOO", false);
            JexlNodeAssert nodeAssert = JexlNodeAssert.assertThat(node)
                            .isInstanceOf(ASTAssignment.class)
                            .hasNumChildren(2)
                            .hasValidLineage();
        
            nodeAssert.child(0)
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .child(0)
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO");
        
            nodeAssert.child(1)
                            .isInstanceOf(ASTFalseNode.class);
        }
    
        /**
         * Verify that {@link JexlNodeFactory#createAssignment(String, String)} returns a valid assignment node.
         */
        @Test
        public void testStringValue() {
            ASTAssignment node = JexlNodeFactory.createAssignment("FOO", "Georgia");
            JexlNodeAssert nodeAssert = JexlNodeAssert.assertThat(node)
                            .isInstanceOf(ASTAssignment.class)
                            .hasNumChildren(2)
                            .hasValidLineage();
        
            nodeAssert.child(0)
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .child(0)
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO");
        
            nodeAssert.child(1)
                            .isInstanceOf(ASTStringLiteral.class)
                            .hasImage("Georgia");
        }
    
        /**
         * Verify that {@link JexlNodeFactory#createAssignment(String, String)} throws an exception when given a null string value.
         */
        @Test
        public void testNullStringValue() {
            assertThrows("value string must not be null", NullPointerException.class, () -> JexlNodeFactory.createAssignment("FOO", null));
        }
    }
}
