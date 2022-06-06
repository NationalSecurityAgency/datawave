package datawave.query.jexl;

import datawave.test.JexlNodeAssert;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import static datawave.test.JexlNodeAssert.assertThat;

@RunWith(Enclosed.class)
public class JexlNodeFactoryTest {
    
    /**
     * Tests for {@link JexlNodeFactory#buildBooleanNode(JexlNode, ASTIdentifier, Boolean)}.
     */
    public static class BuildBooleanNodeTests {
        
        @Test
        public void testEQNodeWithTrue() {
            ASTIdentifier identifier = JexlNodeBuilder.ASTIdentifier().withImage("FOO").build();
            JexlNode node = JexlNodeFactory.buildBooleanNode(JexlNodeInstance.ASTEQNode.create(), identifier, true);
            
            // @formatter:off
            JexlNodeAssert rootAssert = assertThat(node)
                            .isInstanceOf(ASTEQNode.class)
                            .hasNumChildren(2)
                            .hasValidLineage()
                            .hasExactQueryString("FOO == true");
            rootAssert.assertFirstChild()
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertFirstChild()
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO")
                            .hasNoChildren();
            rootAssert.assertSecondChild()
                            .isInstanceOf(ASTTrueNode.class)
                            .hasNoChildren();
            // @formatter:on
        }
        
        @Test
        public void testEQNodeWithFalse() {
            ASTIdentifier identifier = JexlNodeBuilder.ASTIdentifier().withImage("FOO").build();
            JexlNode node = JexlNodeFactory.buildBooleanNode(JexlNodeInstance.ASTEQNode.create(), identifier, false);
            
            // @formatter:off
            JexlNodeAssert rootAssert = assertThat(node)
                            .isInstanceOf(ASTEQNode.class)
                            .hasNumChildren(2)
                            .hasValidLineage()
                            .hasExactQueryString("FOO == false");
            rootAssert.assertFirstChild()
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertFirstChild()
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO")
                            .hasNoChildren();
            rootAssert.assertSecondChild()
                            .isInstanceOf(ASTFalseNode.class)
                            .hasNoChildren();
            // @formatter:on
        }
        
        @Test
        public void testNENodeWithTrue() {
            ASTIdentifier identifier = JexlNodeBuilder.ASTIdentifier().withImage("FOO").build();
            JexlNode node = JexlNodeFactory.buildBooleanNode(JexlNodeInstance.ASTNENode.create(), identifier, true);
            
            // @formatter:off
            JexlNodeAssert rootAssert = assertThat(node)
                            .isInstanceOf(ASTNENode.class)
                            .hasNumChildren(2)
                            .hasValidLineage()
                            .hasExactQueryString("FOO != true");
            rootAssert.assertFirstChild()
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertFirstChild()
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO")
                            .hasNoChildren();
            rootAssert.assertSecondChild()
                            .isInstanceOf(ASTTrueNode.class)
                            .hasNoChildren();
            // @formatter:on
        }
        
        @Test
        public void testNENodeWithFalse() {
            ASTIdentifier identifier = JexlNodeBuilder.ASTIdentifier().withImage("FOO").build();
            JexlNode node = JexlNodeFactory.buildBooleanNode(JexlNodeInstance.ASTNENode.create(), identifier, false);
            
            // @formatter:off
            JexlNodeAssert rootAssert = assertThat(node)
                            .isInstanceOf(ASTNENode.class)
                            .hasNumChildren(2)
                            .hasValidLineage()
                            .hasExactQueryString("FOO != false");
            rootAssert.assertFirstChild()
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertFirstChild()
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO")
                            .hasNoChildren();
            rootAssert.assertSecondChild()
                            .isInstanceOf(ASTFalseNode.class)
                            .hasNoChildren();
            // @formatter:on
        }
        
        @Test
        public void testERNodeWithTrue() {
            ASTIdentifier identifier = JexlNodeBuilder.ASTIdentifier().withImage("FOO").build();
            JexlNode node = JexlNodeFactory.buildBooleanNode(JexlNodeInstance.ASTERNode.create(), identifier, true);
            
            // @formatter:off
            JexlNodeAssert rootAssert = assertThat(node)
                            .isInstanceOf(ASTERNode.class)
                            .hasNumChildren(2)
                            .hasValidLineage()
                            .hasExactQueryString("FOO =~ true");
            rootAssert.assertFirstChild()
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertFirstChild()
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO")
                            .hasNoChildren();
            rootAssert.assertSecondChild()
                            .isInstanceOf(ASTTrueNode.class)
                            .hasNoChildren();
            // @formatter:on
        }
        
        @Test
        public void testERNodeWithFalse() {
            ASTIdentifier identifier = JexlNodeBuilder.ASTIdentifier().withImage("FOO").build();
            JexlNode node = JexlNodeFactory.buildBooleanNode(JexlNodeInstance.ASTERNode.create(), identifier, false);
            
            // @formatter:off
            JexlNodeAssert rootAssert = assertThat(node)
                            .isInstanceOf(ASTERNode.class)
                            .hasNumChildren(2)
                            .hasValidLineage()
                            .hasExactQueryString("FOO =~ false");
            rootAssert.assertFirstChild()
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertFirstChild()
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO")
                            .hasNoChildren();
            rootAssert.assertSecondChild()
                            .isInstanceOf(ASTFalseNode.class)
                            .hasNoChildren();
            // @formatter:on
        }
        
        @Test
        public void testNRNodeWithTrue() {
            ASTIdentifier identifier = JexlNodeBuilder.ASTIdentifier().withImage("FOO").build();
            JexlNode node = JexlNodeFactory.buildBooleanNode(JexlNodeInstance.ASTNRNode.create(), identifier, true);
            
            // @formatter:off
            JexlNodeAssert rootAssert = assertThat(node)
                            .isInstanceOf(ASTNRNode.class)
                            .hasNumChildren(2)
                            .hasValidLineage()
                            .hasExactQueryString("FOO !~ true");
            rootAssert.assertFirstChild()
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertFirstChild()
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO")
                            .hasNoChildren();
            rootAssert.assertSecondChild()
                            .isInstanceOf(ASTTrueNode.class)
                            .hasNoChildren();
            // @formatter:on
        }
        
        @Test
        public void testNRNodeWithFalse() {
            ASTIdentifier identifier = JexlNodeBuilder.ASTIdentifier().withImage("FOO").build();
            JexlNode node = JexlNodeFactory.buildBooleanNode(JexlNodeInstance.ASTNRNode.create(), identifier, false);
            
            // @formatter:off
            JexlNodeAssert rootAssert = assertThat(node)
                            .isInstanceOf(ASTNRNode.class)
                            .hasNumChildren(2)
                            .hasValidLineage()
                            .hasExactQueryString("FOO !~ false");
            rootAssert.assertFirstChild()
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertFirstChild()
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO")
                            .hasNoChildren();
            rootAssert.assertSecondChild()
                            .isInstanceOf(ASTFalseNode.class)
                            .hasNoChildren();
            // @formatter:on
        }
        
        @Test
        public void testGTNodeWithTrue() {
            ASTIdentifier identifier = JexlNodeBuilder.ASTIdentifier().withImage("FOO").build();
            JexlNode node = JexlNodeFactory.buildBooleanNode(JexlNodeInstance.ASTGTNode.create(), identifier, true);
            
            // @formatter:off
            JexlNodeAssert rootAssert = assertThat(node)
                            .isInstanceOf(ASTGTNode.class)
                            .hasNumChildren(2)
                            .hasValidLineage()
                            .hasExactQueryString("FOO > true");
            rootAssert.assertFirstChild()
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertFirstChild()
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO")
                            .hasNoChildren();
            rootAssert.assertSecondChild()
                            .isInstanceOf(ASTTrueNode.class)
                            .hasNoChildren();
            // @formatter:on
        }
        
        @Test
        public void testGTNodeWithFalse() {
            ASTIdentifier identifier = JexlNodeBuilder.ASTIdentifier().withImage("FOO").build();
            JexlNode node = JexlNodeFactory.buildBooleanNode(JexlNodeInstance.ASTGTNode.create(), identifier, false);
            
            // @formatter:off
            JexlNodeAssert rootAssert = assertThat(node)
                            .isInstanceOf(ASTGTNode.class)
                            .hasNumChildren(2)
                            .hasValidLineage()
                            .hasExactQueryString("FOO > false");
            rootAssert.assertFirstChild()
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertFirstChild()
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO")
                            .hasNoChildren();
            rootAssert.assertSecondChild()
                            .isInstanceOf(ASTFalseNode.class)
                            .hasNoChildren();
            // @formatter:on
        }
        
        @Test
        public void testGENodeWithTrue() {
            ASTIdentifier identifier = JexlNodeBuilder.ASTIdentifier().withImage("FOO").build();
            JexlNode node = JexlNodeFactory.buildBooleanNode(JexlNodeInstance.ASTGENode.create(), identifier, true);
            
            // @formatter:off
            JexlNodeAssert rootAssert = assertThat(node)
                            .isInstanceOf(ASTGENode.class)
                            .hasNumChildren(2)
                            .hasValidLineage()
                            .hasExactQueryString("FOO >= true");
            rootAssert.assertFirstChild()
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertFirstChild()
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO")
                            .hasNoChildren();
            rootAssert.assertSecondChild()
                            .isInstanceOf(ASTTrueNode.class)
                            .hasNoChildren();
            // @formatter:on
        }
        
        @Test
        public void testGENodeWithFalse() {
            ASTIdentifier identifier = JexlNodeBuilder.ASTIdentifier().withImage("FOO").build();
            JexlNode node = JexlNodeFactory.buildBooleanNode(JexlNodeInstance.ASTGENode.create(), identifier, false);
            
            // @formatter:off
            JexlNodeAssert rootAssert = assertThat(node)
                            .isInstanceOf(ASTGENode.class)
                            .hasNumChildren(2)
                            .hasValidLineage()
                            .hasExactQueryString("FOO >= false");
            rootAssert.assertFirstChild()
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertFirstChild()
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO")
                            .hasNoChildren();
            rootAssert.assertSecondChild()
                            .isInstanceOf(ASTFalseNode.class)
                            .hasNoChildren();
            // @formatter:on
        }
        
        @Test
        public void testLTNodeWithTrue() {
            ASTIdentifier identifier = JexlNodeBuilder.ASTIdentifier().withImage("FOO").build();
            JexlNode node = JexlNodeFactory.buildBooleanNode(JexlNodeInstance.ASTLTNode.create(), identifier, true);
            
            // @formatter:off
            JexlNodeAssert rootAssert = assertThat(node)
                            .isInstanceOf(ASTLTNode.class)
                            .hasNumChildren(2)
                            .hasValidLineage()
                            .hasExactQueryString("FOO < true");
            rootAssert.assertFirstChild()
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertFirstChild()
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO")
                            .hasNoChildren();
            rootAssert.assertSecondChild()
                            .isInstanceOf(ASTTrueNode.class)
                            .hasNoChildren();
            // @formatter:on
        }
        
        @Test
        public void testLTNodeWithFalse() {
            ASTIdentifier identifier = JexlNodeBuilder.ASTIdentifier().withImage("FOO").build();
            JexlNode node = JexlNodeFactory.buildBooleanNode(JexlNodeInstance.ASTLTNode.create(), identifier, false);
            
            // @formatter:off
            JexlNodeAssert rootAssert = assertThat(node)
                            .isInstanceOf(ASTLTNode.class)
                            .hasNumChildren(2)
                            .hasValidLineage()
                            .hasExactQueryString("FOO < false");
            rootAssert.assertFirstChild()
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertFirstChild()
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO")
                            .hasNoChildren();
            rootAssert.assertSecondChild()
                            .isInstanceOf(ASTFalseNode.class)
                            .hasNoChildren();
            // @formatter:on
        }
        
        @Test
        public void testLENodeWithTrue() {
            ASTIdentifier identifier = JexlNodeBuilder.ASTIdentifier().withImage("FOO").build();
            JexlNode node = JexlNodeFactory.buildBooleanNode(JexlNodeInstance.ASTLENode.create(), identifier, true);
            
            // @formatter:off
            JexlNodeAssert rootAssert = assertThat(node)
                            .isInstanceOf(ASTLENode.class)
                            .hasNumChildren(2)
                            .hasValidLineage()
                            .hasExactQueryString("FOO <= true");
            rootAssert.assertFirstChild()
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertFirstChild()
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO")
                            .hasNoChildren();
            rootAssert.assertSecondChild()
                            .isInstanceOf(ASTTrueNode.class)
                            .hasNoChildren();
            // @formatter:on
        }
        
        @Test
        public void testLENodeWithFalse() {
            ASTIdentifier identifier = JexlNodeBuilder.ASTIdentifier().withImage("FOO").build();
            JexlNode node = JexlNodeFactory.buildBooleanNode(JexlNodeInstance.ASTLENode.create(), identifier, false);
            
            // @formatter:off
            JexlNodeAssert rootAssert = assertThat(node)
                            .isInstanceOf(ASTLENode.class)
                            .hasNumChildren(2)
                            .hasValidLineage()
                            .hasExactQueryString("FOO <= false");
            rootAssert.assertFirstChild()
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertFirstChild()
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO")
                            .hasNoChildren();
            rootAssert.assertSecondChild()
                            .isInstanceOf(ASTFalseNode.class)
                            .hasNoChildren();
            // @formatter:on
        }
        
        @Test
        public void testNonComparisonNode() {
            ASTIdentifier identifier = JexlNodeBuilder.ASTIdentifier().withImage("FOO").build();
            Assertions.assertThatExceptionOfType(UnsupportedOperationException.class)
                            .isThrownBy(() -> JexlNodeFactory.buildBooleanNode(JexlNodeInstance.ASTJexlScript.create(), identifier, true))
                            .withMessage("Cannot handle JexlScript");
        }
    }
    
    /**
     * Tests for {@link JexlNodeFactory#buildNullNode(JexlNode, ASTIdentifier)}.
     */
    public static class BuildNullNodeTests {
        
        @Test
        public void testEQNode() {
            ASTIdentifier identifier = JexlNodeBuilder.ASTIdentifier().withImage("FOO").build();
            JexlNode node = JexlNodeFactory.buildNullNode(JexlNodeInstance.ASTEQNode.create(), identifier);
            
            // @formatter:off
            JexlNodeAssert rootAssert = assertThat(node)
                            .isInstanceOf(ASTEQNode.class)
                            .hasNumChildren(2)
                            .hasValidLineage()
                            .hasExactQueryString("FOO == null");
            rootAssert.assertFirstChild()
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertFirstChild()
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO")
                            .hasNoChildren();
            rootAssert.assertSecondChild()
                            .isInstanceOf(ASTNullLiteral.class)
                            .hasNoChildren();
            // @formatter:on
        }
        
        @Test
        public void testNENode() {
            ASTIdentifier identifier = JexlNodeBuilder.ASTIdentifier().withImage("FOO").build();
            JexlNode node = JexlNodeFactory.buildNullNode(JexlNodeInstance.ASTNENode.create(), identifier);
            
            // @formatter:off
            JexlNodeAssert rootAssert = assertThat(node)
                            .isInstanceOf(ASTNENode.class)
                            .hasNumChildren(2)
                            .hasValidLineage()
                            .hasExactQueryString("FOO != null");
            rootAssert.assertFirstChild()
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertFirstChild()
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO")
                            .hasNoChildren();
            rootAssert.assertSecondChild()
                            .isInstanceOf(ASTNullLiteral.class)
                            .hasNoChildren();
            // @formatter:on
        }
        
        @Test
        public void testERNode() {
            ASTIdentifier identifier = JexlNodeBuilder.ASTIdentifier().withImage("FOO").build();
            JexlNode node = JexlNodeFactory.buildNullNode(JexlNodeInstance.ASTERNode.create(), identifier);
            
            // @formatter:off
            JexlNodeAssert rootAssert = assertThat(node)
                            .isInstanceOf(ASTERNode.class)
                            .hasNumChildren(2)
                            .hasValidLineage()
                            .hasExactQueryString("FOO =~ null");
            rootAssert.assertFirstChild()
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertFirstChild()
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO")
                            .hasNoChildren();
            rootAssert.assertSecondChild()
                            .isInstanceOf(ASTNullLiteral.class)
                            .hasNoChildren();
            // @formatter:on
        }
        
        @Test
        public void testNRNode() {
            ASTIdentifier identifier = JexlNodeBuilder.ASTIdentifier().withImage("FOO").build();
            JexlNode node = JexlNodeFactory.buildNullNode(JexlNodeInstance.ASTNRNode.create(), identifier);
            
            // @formatter:off
            JexlNodeAssert rootAssert = assertThat(node)
                            .isInstanceOf(ASTNRNode.class)
                            .hasNumChildren(2)
                            .hasValidLineage()
                            .hasExactQueryString("FOO !~ null");
            rootAssert.assertFirstChild()
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertFirstChild()
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO")
                            .hasNoChildren();
            rootAssert.assertSecondChild()
                            .isInstanceOf(ASTNullLiteral.class)
                            .hasNoChildren();
            // @formatter:on
        }
        
        @Test
        public void testGTNode() {
            ASTIdentifier identifier = JexlNodeBuilder.ASTIdentifier().withImage("FOO").build();
            JexlNode node = JexlNodeFactory.buildNullNode(JexlNodeInstance.ASTGTNode.create(), identifier);
            
            // @formatter:off
            JexlNodeAssert rootAssert = assertThat(node)
                            .isInstanceOf(ASTGTNode.class)
                            .hasNumChildren(2)
                            .hasValidLineage()
                            .hasExactQueryString("FOO > null");
            rootAssert.assertFirstChild()
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertFirstChild()
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO")
                            .hasNoChildren();
            rootAssert.assertSecondChild()
                            .isInstanceOf(ASTNullLiteral.class)
                            .hasNoChildren();
            // @formatter:on
        }
        
        @Test
        public void testGENode() {
            ASTIdentifier identifier = JexlNodeBuilder.ASTIdentifier().withImage("FOO").build();
            JexlNode node = JexlNodeFactory.buildNullNode(JexlNodeInstance.ASTGENode.create(), identifier);
            
            // @formatter:off
            JexlNodeAssert rootAssert = assertThat(node)
                            .isInstanceOf(ASTGENode.class)
                            .hasNumChildren(2)
                            .hasValidLineage()
                            .hasExactQueryString("FOO >= null");
            rootAssert.assertFirstChild()
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertFirstChild()
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO")
                            .hasNoChildren();
            rootAssert.assertSecondChild()
                            .isInstanceOf(ASTNullLiteral.class)
                            .hasNoChildren();
            // @formatter:on
        }
        
        @Test
        public void testLTNode() {
            ASTIdentifier identifier = JexlNodeBuilder.ASTIdentifier().withImage("FOO").build();
            JexlNode node = JexlNodeFactory.buildNullNode(JexlNodeInstance.ASTLTNode.create(), identifier);
            
            // @formatter:off
            JexlNodeAssert rootAssert = assertThat(node)
                            .isInstanceOf(ASTLTNode.class)
                            .hasNumChildren(2)
                            .hasValidLineage()
                            .hasExactQueryString("FOO < null");
            rootAssert.assertFirstChild()
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertFirstChild()
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO")
                            .hasNoChildren();
            rootAssert.assertSecondChild()
                            .isInstanceOf(ASTNullLiteral.class)
                            .hasNoChildren();
            // @formatter:on
        }
        
        @Test
        public void testLENode() {
            ASTIdentifier identifier = JexlNodeBuilder.ASTIdentifier().withImage("FOO").build();
            JexlNode node = JexlNodeFactory.buildNullNode(JexlNodeInstance.ASTLENode.create(), identifier);
            
            // @formatter:off
            JexlNodeAssert rootAssert = assertThat(node)
                            .isInstanceOf(ASTLENode.class)
                            .hasNumChildren(2)
                            .hasValidLineage()
                            .hasExactQueryString("FOO <= null");
            rootAssert.assertFirstChild()
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertFirstChild()
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO")
                            .hasNoChildren();
            rootAssert.assertSecondChild()
                            .isInstanceOf(ASTNullLiteral.class)
                            .hasNoChildren();
            // @formatter:on
        }
        
        @Test
        public void testNonComparisonNode() {
            ASTIdentifier identifier = JexlNodeBuilder.ASTIdentifier().withImage("FOO").build();
            Assertions.assertThatExceptionOfType(UnsupportedOperationException.class)
                            .isThrownBy(() -> JexlNodeFactory.buildNullNode(JexlNodeInstance.ASTJexlScript.create(), identifier))
                            .withMessage("Cannot handle JexlScript");
        }
    }
    
    /**
     * Tests for {@link JexlNodeFactory#buildIdentifier(String)}.
     */
    public static class BuildIdentifierTests {
        
        @Test
        public void testValidIdentifier() {
            ASTIdentifier node = JexlNodeFactory.buildIdentifier("FOO");
            assertThat(node).hasNoChildren().hasNullParent().isInstanceOf(ASTIdentifier.class).hasImage("FOO");
        }
        
        @Test
        public void testInvalidIdentifier() {
            ASTIdentifier node = JexlNodeFactory.buildIdentifier("!FOO");
            assertThat(node).hasNoChildren().hasNullParent().isInstanceOf(ASTIdentifier.class).hasImage("$!FOO");
        }
    }
    
    /**
     * Tests for {@link JexlNodeFactory#createAssignment(String, boolean)} and {@link JexlNodeFactory#createAssignment(String, String)}.
     */
    public static class CreateAssignmentTests {
        
        @Test
        public void testTrueValue() {
            ASTAssignment node = JexlNodeFactory.createAssignment("FOO", true);
            // @formatter:off
            JexlNodeAssert rootAssert = assertThat(node)
                            .hasNumChildren(2)
                            .hasValidLineage()
                            .hasExactQueryString("(FOO = true)");
            rootAssert.assertFirstChild()
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertFirstChild()
                                .isInstanceOf(ASTIdentifier.class)
                                .hasImage("FOO")
                                .hasNoChildren();
            rootAssert.assertSecondChild()
                            .isInstanceOf(ASTTrueNode.class)
                            .hasNoChildren();
            // @formatter:on
        }
        
        @Test
        public void testFalseValue() {
            ASTAssignment node = JexlNodeFactory.createAssignment("FOO", false);
            
            // @formatter:off
            JexlNodeAssert rootAssert = assertThat(node)
                            .hasNumChildren(2)
                            .hasValidLineage()
                            .hasExactQueryString("(FOO = false)");
            rootAssert.assertFirstChild()
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertFirstChild()
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO")
                            .hasNoChildren();
            rootAssert.assertSecondChild()
                            .isInstanceOf(ASTFalseNode.class)
                            .hasNoChildren();
            // @formatter:on
        }
        
        @Test
        public void testStringValue() {
            ASTAssignment node = JexlNodeFactory.createAssignment("FOO", "abc");
            
            // @formatter:off
            JexlNodeAssert rootAssert = assertThat(node)
                            .hasNumChildren(2)
                            .hasValidLineage()
                            .hasExactQueryString("(FOO = 'abc')");
            rootAssert.assertFirstChild()
                            .isInstanceOf(ASTReference.class)
                            .hasNumChildren(1)
                            .assertFirstChild()
                            .isInstanceOf(ASTIdentifier.class)
                            .hasImage("FOO")
                            .hasNoChildren();
            rootAssert.assertSecondChild()
                            .isInstanceOf(ASTStringLiteral.class)
                            .hasImage("abc")
                            .hasNoChildren();
            // @formatter:on
        }
        
    }
}
