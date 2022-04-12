package datawave.query.jexl;

import datawave.query.jexl.visitors.PrintingVisitor;
import datawave.test.JexlNodeAssert;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertThrows;

@RunWith(Enclosed.class)
public class JexlNodeFactoryTest {
    
    public static class QuestionableBehaviorCases {
        
        /**
         * Method: {@link JexlNodeFactory#createExpression(JexlNode)}
         * A reference child with a non-reference expression child results in the original reference wrapped in new reference and reference
         * expression nodes. Should the original reference child have a reference expression inserted after it if it only has one child?
         */
        @Test
        public void testCreateExpressionWithReferenceChildWithoutReferenceExpression() {
            ASTIdentifier identifier = new ASTIdentifier(ParserTreeConstants.JJTIDENTIFIER);
            identifier.image = "FOO";
        
            ASTReference reference = new ASTReference(ParserTreeConstants.JJTREFERENCE);
            reference.jjtAddChild(identifier, 0);
            identifier.jjtSetParent(reference);
        
            JexlNode node = JexlNodeFactory.createExpression(reference);
        
            // @formatter:off
            JexlNodeAssert.assertThat(node).isInstanceOf(ASTReference.class)
                            .hasValidLineage()
                            .hasNumChildren(1)
                            .assertChild(0)
                            .isInstanceOf(ASTReferenceExpression.class)
                            .hasNumChildren(1)
                            .assertChild(0)
                            .isSameAs(reference);
            // @formatter:on
        }
    }
    
    /**
     * Tests for {@link JexlNodeFactory#createExpression(JexlNode)} and {@link JexlNodeFactory#createExpression(JexlNode, JexlNode)}.
     */
    public static class CreateExpressionTests {
    
        /**
         * Verify a null inputs result in exceptions.
         */
        @Test
        public void testNullInputs() {
            assertThrows("child must not be null", NullPointerException.class, () -> JexlNodeFactory.createExpression(null));
            assertThrows("child container must not be null", NullPointerException.class, () -> JexlNodeFactory.createExpression(null, new ASTReference(ParserTreeConstants.JJTREFERENCE)));
            assertThrows("wrapping container must not be null", NullPointerException.class, () -> JexlNodeFactory.createExpression(new ASTReference(ParserTreeConstants.JJTREFERENCE), null));
        }
    
        /**
         * Method: {@link JexlNodeFactory#createExpression(JexlNode)}
         * Verify that a non-reference child results in it being returned wrapped in reference and reference expression nodes.
         */
        @Test
        public void testNonReferenceChild() {
            ASTIdentifier identifier = new ASTIdentifier(ParserTreeConstants.JJTIDENTIFIER);
            identifier.image = "FOO";
    
            JexlNode node = JexlNodeFactory.createExpression(identifier);
    
            // @formatter:off
            JexlNodeAssert.assertThat(node).isInstanceOf(ASTReference.class)
                            .hasValidLineage()
                            .hasNumChildren(1)
                            .assertChild(0)
                            .isInstanceOf(ASTReferenceExpression.class)
                            .hasNumChildren(1)
                            .assertChild(0)
                            .isSameAs(identifier);
            // @formatter:on
        }
    
        /**
         * Method: {@link JexlNodeFactory#createExpression(JexlNode)}
         * Verify that a reference expression child results in it being returned wrapped in a reference node.
         */
        @Test
        public void testReferenceExpressionChild() {
            ASTIdentifier identifier = new ASTIdentifier(ParserTreeConstants.JJTIDENTIFIER);
            identifier.image = "FOO";
    
            ASTReferenceExpression referenceExpression = new ASTReferenceExpression(ParserTreeConstants.JJTREFERENCEEXPRESSION);
            referenceExpression.jjtAddChild(identifier, 0);
            identifier.jjtSetParent(referenceExpression);
    
            JexlNode node = JexlNodeFactory.createExpression(referenceExpression);
            
            // @formatter:off
            JexlNodeAssert.assertThat(node).isInstanceOf(ASTReference.class)
                            .hasValidLineage()
                            .hasNumChildren(1)
                            .assertChild(0)
                            .isSameAs(referenceExpression)
                            .hasNumChildren(1)
                            .assertChild(0)
                            .isSameAs(identifier);
            // @formatter:on
        }
    
        /**
         * Method: {@link JexlNodeFactory#createExpression(JexlNode)}
         * Verify that a reference child with a reference expression child results in the original reference child being returned.
         */
        @Test
        public void testReferenceWithReferenceExpressionChild() {
            ASTIdentifier identifier = new ASTIdentifier(ParserTreeConstants.JJTIDENTIFIER);
            identifier.image = "FOO";
    
            ASTReferenceExpression referenceExpression = new ASTReferenceExpression(ParserTreeConstants.JJTREFERENCEEXPRESSION);
            referenceExpression.jjtAddChild(identifier, 0);
            identifier.jjtSetParent(referenceExpression);
    
            ASTReference reference = new ASTReference(ParserTreeConstants.JJTREFERENCE);
            reference.jjtAddChild(referenceExpression, 0);
            referenceExpression.jjtSetParent(reference);
    
            JexlNode node = JexlNodeFactory.createExpression(referenceExpression);
    
            // @formatter:off
            JexlNodeAssert.assertThat(node).isInstanceOf(ASTReference.class)
                            .hasValidLineage()
                            .hasNumChildren(1)
                            .assertChild(0)
                            .isSameAs(referenceExpression)
                            .hasNumChildren(1)
                            .assertChild(0)
                            .isSameAs(identifier);
            // @formatter:on
        }
    
        /**
         * Method: {@link JexlNodeFactory#createExpression(JexlNode, JexlNode)}
         * Verify that a valid expression is created for a child and wrapping container.
         */
        @Test
        public void testCreateExpressionWithContainers() {
            ASTOrNode childContainer = new ASTOrNode(ParserTreeConstants.JJTORNODE);
            
            ASTAssignment assignment1 = JexlNodeFactory.createAssignment("FOO", true);
            childContainer.jjtAddChild(assignment1, 0);
            assignment1.jjtSetParent(childContainer);
            
            ASTAssignment assignment2 = JexlNodeFactory.createAssignment("BAR", false);
            childContainer.jjtAddChild(assignment2, 1);
            assignment2.jjtSetParent(childContainer);
            
            ASTAndNode wrappingContainer = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
    
            JexlNode node = JexlNodeFactory.createExpression(childContainer, wrappingContainer);
    
            PrintingVisitor.printQuery(node);
            
            // @formatter:off
            JexlNodeAssert refExpressionAssert = JexlNodeAssert.assertThat(node)
                            .isInstanceOf(ASTAndNode.class)
                            .hasNumChildren(1)
                            .hasValidLineage()
                            .assertChild(0)
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertChild(0)
                            .isInstanceOf(ASTReferenceExpression.class)
                            .hasNumChildren(2);
            
            refExpressionAssert.assertChild(0).isSameAs(assignment1);
            refExpressionAssert.assertChild(1).isSameAs(assignment2);
            // @formatter:on
        }
    }
    
    /**
     * Tests for {@link JexlNodeFactory#createAssignment(String, String)} and {@link JexlNodeFactory#createAssignment(String, boolean)}.
     */
    public static class CreateAssignmentTests {
    
        /**
         * Method: {@link JexlNodeFactory#createAssignment(String, boolean)}
         * Method: {@link JexlNodeFactory#createAssignment(String, String)}
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
         * Method: {@link JexlNodeFactory#createAssignment(String, boolean)}
         * Verify that a valid assignment node is returned for the boolean value true.
         */
        @Test
        public void testBooleanValueOfTrue() {
            ASTAssignment node = JexlNodeFactory.createAssignment("FOO", true);
            // @formatter:off
            JexlNodeAssert nodeAssert = JexlNodeAssert.assertThat(node)
                            .isInstanceOf(ASTAssignment.class)
                            .hasNumChildren(2)
                            .hasValidLineage();
            
            nodeAssert.assertChild(0)
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertChild(0)
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO");
            
            nodeAssert.assertChild(1)
                            .isInstanceOf(ASTTrueNode.class);
            // @formatter:on
        }
    
        /**
         * Method: {@link JexlNodeFactory#createAssignment(String, boolean)}
         * Verify that a valid assignment node is returned for the boolean value false.
         */
        @Test
        public void testBooleanValueOfFalse() {
            ASTAssignment node = JexlNodeFactory.createAssignment("FOO", false);
            // @formatter:off
            JexlNodeAssert nodeAssert = JexlNodeAssert.assertThat(node)
                            .isInstanceOf(ASTAssignment.class)
                            .hasNumChildren(2)
                            .hasValidLineage();
        
            nodeAssert.assertChild(0)
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertChild(0)
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO");
        
            nodeAssert.assertChild(1)
                            .isInstanceOf(ASTFalseNode.class);
            // @formatter:on
        }
    
        /**
         * Method: {@link JexlNodeFactory#createAssignment(String, String)}
         * Verify that a valid assignment node is returned for a non-blank value.
         */
        @Test
        public void testStringValue() {
            ASTAssignment node = JexlNodeFactory.createAssignment("FOO", "Georgia");
            // @formatter:off
            JexlNodeAssert nodeAssert = JexlNodeAssert.assertThat(node)
                            .isInstanceOf(ASTAssignment.class)
                            .hasNumChildren(2)
                            .hasValidLineage();
        
            nodeAssert.assertChild(0)
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertChild(0)
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO");
        
            nodeAssert.assertChild(1)
                            .isInstanceOf(ASTStringLiteral.class)
                            .hasImage("Georgia");
            // @formatter:on
        }
    
        /**
         * Method: {@link JexlNodeFactory#createAssignment(String, String)}
         * Verify that a null string value results in an exception.
         */
        @Test
        public void testNullStringValue() {
            assertThrows("value string must not be null", NullPointerException.class, () -> JexlNodeFactory.createAssignment("FOO", null));
        }
    }
}
