package datawave.data.normalizer.regex.visitor;

import datawave.data.normalizer.regex.AlternationNode;
import datawave.data.normalizer.regex.AnyCharNode;
import datawave.data.normalizer.regex.CharClassNode;
import datawave.data.normalizer.regex.CharRangeNode;
import datawave.data.normalizer.regex.DigitCharClassNode;
import datawave.data.normalizer.regex.EmptyNode;
import datawave.data.normalizer.regex.EncodedNumberNode;
import datawave.data.normalizer.regex.EncodedPatternNode;
import datawave.data.normalizer.regex.EndAnchorNode;
import datawave.data.normalizer.regex.EscapedSingleCharNode;
import datawave.data.normalizer.regex.ExpressionNode;
import datawave.data.normalizer.regex.GroupNode;
import datawave.data.normalizer.regex.IntegerNode;
import datawave.data.normalizer.regex.IntegerRangeNode;
import datawave.data.normalizer.regex.OneOrMoreNode;
import datawave.data.normalizer.regex.QuestionMarkNode;
import datawave.data.normalizer.regex.RepetitionNode;
import datawave.data.normalizer.regex.SingleCharNode;
import datawave.data.normalizer.regex.StartAnchorNode;
import datawave.data.normalizer.regex.ZeroOrMoreNode;

public interface Visitor {
    
    Object visitExpression(ExpressionNode node, Object data);
    
    Object visitAlternation(AlternationNode node, Object data);
    
    Object visitGroup(GroupNode node, Object data);
    
    Object visitDigitChar(DigitCharClassNode node, Object data);
    
    Object visitCharClass(CharClassNode node, Object data);
    
    Object visitCharRange(CharRangeNode node, Object data);
    
    Object visitSingleChar(SingleCharNode node, Object data);
    
    Object visitEscapedSingleChar(EscapedSingleCharNode node, Object data);
    
    Object visitRepetition(RepetitionNode node, Object data);
    
    Object visitQuestionMark(QuestionMarkNode node, Object data);
    
    Object visitAnyChar(AnyCharNode node, Object data);
    
    Object visitZeroToMany(ZeroOrMoreNode node, Object data);
    
    Object visitOneToMany(OneOrMoreNode node, Object data);
    
    Object visitInteger(IntegerNode node, Object data);
    
    Object visitIntegerRange(IntegerRangeNode node, Object data);
    
    Object visitEmpty(EmptyNode node, Object data);
    
    Object visitStartAnchor(StartAnchorNode node, Object data);
    
    Object visitEndAnchor(EndAnchorNode node, Object data);
    
    Object visitEncodedNumber(EncodedNumberNode node, Object data);
    
    Object visitEncodedPattern(EncodedPatternNode node, Object data);
}
