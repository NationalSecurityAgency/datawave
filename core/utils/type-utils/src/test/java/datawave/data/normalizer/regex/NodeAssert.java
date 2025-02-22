package datawave.data.normalizer.regex;

import java.util.Objects;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.Assertions;

import datawave.data.normalizer.regex.visitor.EqualityVisitor;
import datawave.data.normalizer.regex.visitor.PrintVisitor;
import datawave.data.normalizer.regex.visitor.StringVisitor;

public class NodeAssert<SELF extends AbstractAssert<SELF,ACTUAL>,ACTUAL extends Node> extends AbstractAssert<SELF,ACTUAL> {
    
    public static NodeAssert<?,?> assertThat(Node node) {
        return new NodeAssert<>(node);
    }
    
    protected NodeAssert(ACTUAL actual) {
        super(actual, NodeAssert.class);
    }
    
    protected NodeAssert(ACTUAL actual, Class<?> selfType) {
        super(actual, selfType);
    }
    
    public NodeAssert<SELF,ACTUAL> hasNullParent() {
        isNotNull();
        Node parent = actual.getParent();
        if (parent != null) {
            failWithMessage("Expected parent to be null, but was %s", parent);
        }
        return this;
    }
    
    public NodeAssert<SELF,ACTUAL> hasNonNullParent() {
        isNotNull();
        if (actual.getParent() == null) {
            failWithMessage("Expected parent to be non-null");
        }
        return this;
    }
    
    public NodeAssert<SELF,ACTUAL> hasChildCount(int count) {
        isNotNull();
        int actualCount = actual.getChildCount();
        if (count != actualCount) {
            failWithMessage("Expected child count to be %d, but was %d", count, actualCount);
        }
        return this;
    }
    
    public NodeAssert<SELF,ACTUAL> hasNoChildren() {
        return hasChildCount(0);
    }
    
    public NodeAssert<?,?> assertChild(int childIndex) {
        isNotNull();
        int childCount = actual.getChildCount();
        if (childIndex >= childCount) {
            failWithMessage("Expected to find child at index %d but there are %d children", childIndex, childCount);
        }
        Node child = actual.getChildAt(childIndex);
        return assertThat(child);
    }
    
    public NodeAssert<?,?> assertParent() {
        isNotNull();
        return assertThat(actual.getParent());
    }
    
    public NodeAssert<?,?> assertGrandparent() {
        isNotNull();
        return assertParent().assertParent();
    }
    
    public NodeAssert<SELF,ACTUAL> isEqualTreeTo(Node expected) {
        if (!EqualityVisitor.isEqual(actual, expected)) {
            String actualString = StringVisitor.toString(actual);
            String actualPrint = PrintVisitor.printToString(actual);
            String expectedString = StringVisitor.toString(expected);
            String expectedPrint = PrintVisitor.printToString(expected);
            failWithMessage("Expected actual '%s'\n%s\n\nto be equal to expected '%s'\n%s\n\nbut were different trees", actualString, actualPrint,
                            expectedString, expectedPrint);
        }
        return this;
    }
    
    public NodeAssert<SELF,ACTUAL> isNotEqualTreeTo(Node node) {
        if (EqualityVisitor.isEqual(actual, node)) {
            failWithMessage("Expected %s to not be equal to %s, but were identical trees");
        }
        return this;
    }
    
    public NodeAssert<SELF,ACTUAL> isExpressionNode() {
        isInstanceOf(ExpressionNode.class);
        return this;
    }
    
    public NodeAssert<SELF,ACTUAL> isAlternationNode() {
        isInstanceOf(AlternationNode.class);
        return this;
    }
    
    public NodeAssert<SELF,ACTUAL> isGroupNode() {
        isInstanceOf(GroupNode.class);
        return this;
    }
    
    public CharClassNodeAssert isCharClassNode() {
        isInstanceOf(CharClassNode.class);
        return new CharClassNodeAssert((CharClassNode) actual);
    }
    
    public NodeAssert<SELF,ACTUAL> isDigitCharClassNode() {
        isInstanceOf(DigitCharClassNode.class);
        return this;
    }
    
    public NodeAssert<SELF,ACTUAL> isRepetitionNode() {
        isInstanceOf(RepetitionNode.class);
        return this;
    }
    
    public NodeAssert<SELF,ACTUAL> isAnyCharNode() {
        isInstanceOf(AnyCharNode.class);
        return this;
    }
    
    public NodeAssert<SELF,ACTUAL> isZeroToManyNode() {
        isInstanceOf(ZeroOrMoreNode.class);
        return this;
    }
    
    public NodeAssert<SELF,ACTUAL> isOneToManyNode() {
        isInstanceOf(OneOrMoreNode.class);
        return this;
    }
    
    public NodeAssert<SELF,ACTUAL> isOptionalNode() {
        isInstanceOf(QuestionMarkNode.class);
        return this;
    }
    
    public NodeAssert<SELF,ACTUAL> isEmptyNode() {
        isInstanceOf(EmptyNode.class);
        return this;
    }
    
    public SingleCharNodeAssert isSingleCharNode() {
        isInstanceOf(SingleCharNode.class);
        return new SingleCharNodeAssert((SingleCharNode) actual);
    }
    
    public CharRangeNodeAssert isCharRangeNode() {
        isInstanceOf(CharRangeNode.class);
        return new CharRangeNodeAssert((CharRangeNode) actual);
    }
    
    public IntegerNodeAssert isIntegerNode() {
        isInstanceOf(IntegerNode.class);
        return new IntegerNodeAssert((IntegerNode) actual);
    }
    
    public IntegerRangeNodeAssert isIntegerRangeNode() {
        isInstanceOf(IntegerRangeNode.class);
        return new IntegerRangeNodeAssert((IntegerRangeNode) actual);
    }
    
    public NodeAssert<SELF,ACTUAL> isStartAnchorNode() {
        isInstanceOf(StartAnchorNode.class);
        return this;
    }
    
    public NodeAssert<SELF,ACTUAL> isEndAnchorNode() {
        isInstanceOf(EndAnchorNode.class);
        return this;
    }
    
    public EscapedSingleCharNodeAssert isEscapedSingleCharNode() {
        isInstanceOf(EscapedSingleCharNode.class);
        return new EscapedSingleCharNodeAssert((EscapedSingleCharNode) actual);
    }
    
    public NodeAssert<SELF,ACTUAL> isEncodedNumberNode() {
        isInstanceOf(EncodedNumberNode.class);
        return this;
    }
    
    public NodeAssert<SELF,ACTUAL> isEncodedPatternNode() {
        isInstanceOf(EncodedPatternNode.class);
        return this;
    }
    
    public AbstractStringAssert<?> asTreeString() {
        isNotNull();
        return Assertions.assertThat(StringVisitor.toString(actual));
    }
    
    public static class SingleCharNodeAssert extends NodeAssert<SingleCharNodeAssert,SingleCharNode> {
        
        protected SingleCharNodeAssert(SingleCharNode node) {
            super(node, SingleCharNodeAssert.class);
        }
        
        public SingleCharNodeAssert hasCharacter(char expected) {
            isNotNull();
            char actualChar = actual.getCharacter();
            if (!Objects.equals(actualChar, expected)) {
                failWithMessage("Expected character to be %s but was %s", expected, actualChar);
            }
            return this;
        }
    }
    
    public static class EscapedSingleCharNodeAssert extends NodeAssert<EscapedSingleCharNodeAssert,EscapedSingleCharNode> {
        
        protected EscapedSingleCharNodeAssert(EscapedSingleCharNode node) {
            super(node, EscapedSingleCharNodeAssert.class);
        }
        
        public EscapedSingleCharNodeAssert hasCharacter(char expected) {
            isNotNull();
            char actualChar = actual.getCharacter();
            if (!Objects.equals(actualChar, expected)) {
                failWithMessage("Expected character to be %s but was %s", expected, actualChar);
            }
            return this;
        }
    }
    
    public static class CharRangeNodeAssert extends NodeAssert<CharRangeNodeAssert,CharRangeNode> {
        
        protected CharRangeNodeAssert(CharRangeNode node) {
            super(node, CharRangeNodeAssert.class);
        }
        
        public CharRangeNodeAssert hasStart(char expected) {
            isNotNull();
            char actualStart = actual.getStart();
            if (!Objects.equals(expected, actualStart)) {
                failWithMessage("Expected start to be %s but was %s", expected, actualStart);
            }
            return this;
        }
        
        public CharRangeNodeAssert hasEnd(char expected) {
            isNotNull();
            char actualEnd = actual.getEnd();
            if (!Objects.equals(expected, actualEnd)) {
                failWithMessage("Expected end to be %s but was %s", expected, actualEnd);
            }
            return this;
        }
    }
    
    public static class CharClassNodeAssert extends NodeAssert<CharClassNodeAssert,CharClassNode> {
        
        protected CharClassNodeAssert(CharClassNode node) {
            super(node, CharClassNodeAssert.class);
        }
        
        public CharClassNodeAssert isNegated() {
            isNotNull();
            if (!actual.isNegated()) {
                failWithMessage("Expected character class to be negated, but was not");
            }
            return this;
        }
        
        public CharClassNodeAssert isNotNegated() {
            isNotNull();
            if (actual.isNegated()) {
                failWithMessage("Expected character class to not be negated, but was");
            }
            return this;
        }
    }
    
    public static class IntegerNodeAssert extends NodeAssert<IntegerNodeAssert,IntegerNode> {
        
        protected IntegerNodeAssert(IntegerNode node) {
            super(node, IntegerNodeAssert.class);
        }
        
        public IntegerNodeAssert hasValue(int expected) {
            isNotNull();
            int actualValue = actual.getValue();
            if (actualValue != expected) {
                failWithMessage("Expected value to be %d but was %d", expected, actualValue);
            }
            return this;
        }
    }
    
    public static class IntegerRangeNodeAssert extends NodeAssert<IntegerRangeNodeAssert,IntegerRangeNode> {
        
        protected IntegerRangeNodeAssert(IntegerRangeNode node) {
            super(node, IntegerRangeNodeAssert.class);
        }
        
        public IntegerRangeNodeAssert hasStart(Integer expected) {
            isNotNull();
            Integer actualStart = actual.getStart();
            if (!Objects.equals(expected, actualStart)) {
                failWithMessage("Expected start to be %d but was %d", expected, actualStart);
            }
            return this;
        }
        
        public IntegerRangeNodeAssert hasEnd(Integer expected) {
            isNotNull();
            Integer actualEnd = actual.getEnd();
            if (!Objects.equals(expected, actualEnd)) {
                failWithMessage("Expected end to be %d but was %d", expected, actualEnd);
            }
            return this;
        }
        
        public IntegerRangeNodeAssert hasBoundedEnd() {
            isNotNull();
            if (!actual.isEndBounded()) {
                failWithMessage("Expected end to be bounded");
            }
            return this;
        }
        
        public IntegerRangeNodeAssert hasUnboundedEnd() {
            isNotNull();
            if (actual.isEndBounded()) {
                failWithMessage("Expected end to be unbounded but was %d", actual.getEnd());
            }
            return this;
        }
    }
}
