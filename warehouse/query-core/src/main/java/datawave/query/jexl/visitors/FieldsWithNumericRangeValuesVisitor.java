package datawave.query.jexl.visitors;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.lucene.visitors.QueryNodeType;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.lang3.math.NumberUtils;

import datawave.query.jexl.JexlASTHelper;
import org.apache.lucene.queryparser.flexible.core.nodes.NotBooleanQueryNode;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.BOUNDED_RANGE;

/**
 * A visitor that fetches all fields found in the query that have a numeric range as their value.
 */
public class FieldsWithNumericRangeValuesVisitor extends ShortCircuitBaseVisitor {

    /**
     * Fetch all fields that have a numeric range value.
     *
     * @param query
     *            the query
     * @return the set of fields
     */
    @SuppressWarnings("unchecked")
    public static Set<String> getFields(ASTJexlScript query) {
        if (query == null) {
            return Collections.emptySet();
        } else {
            FieldsWithNumericRangeValuesVisitor visitor = new FieldsWithNumericRangeValuesVisitor();
            // Maintain insertion order of fields found.
            return (Set<String>) query.jjtAccept(visitor, new LinkedHashSet<String>());
            // IMPORTANT!!! THE OBJECT data IS WHERE WE'LL SAVE ANY FIELDS WE FIND!!!
        }
    }


    @Override
    public Object visit(ASTLTNode node, Object data) {
        checkSingleField(node, data);
        return data;
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        checkSingleField(node, data);
        return data;
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        checkSingleField(node, data);
        return data;
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        checkSingleField(node, data);
        return data;
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        // if we know from a parent that this is evaluation only (or ignored), pass that forward. if we don't know, check.
        if(QueryPropertyMarker.findInstance(node).isType(BOUNDED_RANGE)) {
           // if the children are both referenceexpressions and this ones parent is then we know wer're in a wonky and node.
            // for each of the refex children we gatta send it back over to the
            //WAIT HUH ok all i need to see is if it's a string range or not and also if it's just a non-numeric value.
            // so not sure how that changes stuff
        }
        return null;
    }

    /*

JexlScript
  ReferenceExpression ((((Bounded = true) && (((foo ge 0) && (foo le 10)))))
    AndNode (((Bounded = true) && (((foo ge 0) && (foo le 10))))
      ReferenceExpression (Bounded = true)
        Assignment -> Bounded = True //TODO WHAT DO I HAVE TO DO TO MAKE THE BOUNDED STUFF POP UP? MOST LIKELY, ILL JUST NEED TO HAVE A CONCRETE BOUNDED RANGE
          _Bounded_:_Bounded_ !! QPM!
          TrueNode
      ReferenceExpression (((foo ge 0) && (foo le 10)))
        AndNode ((foo ge 0) && (foo le 10))
          GENode (foo ge 0)
            FOO:FOO
            0:0
          LENode (foo le 10)
            FOO:FOO
            10:10



We're looking for:

ReferenceNode -> AndNode -> ReferenceNode | ReferenceNode
ReferenceNode -> AndNode -> ComparisonNodes
ReferenceNode -> ComparisonNodes

JexlScript
  ReferenceExpression // DOES THIS EVALUATE TO BOUNDED RANGE?
    GENode
      FOO:FOO
      10:10


JexlScript
  AndNode
    ReferenceExpression
      GTNode
        FOO:FOO
        10:10
    ReferenceExpression
      LENode
        FOO:FOO
        30:30



      // TODO GENIUS!!!!!!!!! THE ASTREFERENCE NODES ARE ALWAYS DIRECTLY RELATED TO JUST THEIR CHILD. THAT'S HOW THEY DO THE MARKING!!!!!
      // TODO THEY HAVE NOTHING TO DO WITH THEMSELVES, THEY MARK THEIR DIRECT CHIULD!!!!!!!!! THEY SHOULDNT HAVE MORE THAN 1 CHILD
     */

    @Override
    public Object visit(ASTReference node, Object data) {
        // if we know from a parent that this is evaluation only (or ignored), pass that forward. if we don't know, check.
        if(QueryPropertyMarker.findInstance(node).isType(BOUNDED_RANGE)) {

            // CHECK IF THE CHILD IS EXTRACTABLE
            /// GT LT GTE LTE
            // IF IT IS, EXTRACT THAT BAD BOY
            JexlNode child = node.jjtGetChild(0);

            // wonky and case
            if(child instanceof ASTAndNode){
                visit((ASTAndNode) child, data);
            }

            // normal other cases
            if(child instanceof ASTGTNode) {
                visit((ASTGTNode)child, data);
            }
            else if (child instanceof ASTGENode) {
                visit((ASTGENode) child, data);
            }
            else if (child instanceof ASTLTNode) {
                visit((ASTLTNode) child, data);
            }
            else if (child instanceof ASTLENode) {
                visit((ASTLENode) child, data);
            }


            /// OTHERWISE WE GATA GO WITH THE AND NODE AND DO FUNKY SHIT

            // extract fields from inside

            /*
            0. we know we're in a refeerence
            1. check if the next one is an and node
            2. if it is, then we move into that bad boy
            3. now that we're in the and node, the
            3b. otherwise, we  do our notmal checking stuff
             */
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    private void checkSingleField(JexlNode node, Object data) {
        // pull the raw string value as long as you can make a range from it
        String field = JexlASTHelper.getIdentifier(node);
        if (field != null) {
            Object literal = JexlASTHelper.getLiteralValue(node);
            if (literal instanceof String) {
                // Track any fields that have a string value that represents a valid number.
                if (NumberUtils.isCreatable((String) literal)) {
                    ((Set<String>) data).add(field);
                }
            }
        }
    }
}
