package datawave.query.predicate;

import java.util.Set;

import javax.annotation.Nullable;

import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTNENode;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.BaseVisitor;

/**
 *
 */
public class NegationPredicate implements Predicate<ASTJexlScript> {

    protected Set<String> indexOnlyFields;

    public NegationPredicate(Set<String> indexOnlyFields) {
        this.indexOnlyFields = indexOnlyFields;
    }

    public NegationPredicate() {
        this.indexOnlyFields = Sets.newHashSet();
    }

    @Override
    public boolean apply(@Nullable ASTJexlScript input) {
        NegationTest test = new NegationTest(indexOnlyFields);
        input.jjtAccept(test, null);
        return test.hasNot();
    }

    private static final class NegationTest extends BaseVisitor {
        private boolean hasNot = false;
        protected Set<String> indexOnlyFields;

        public NegationTest(Set<String> indexOnlyFields) {
            hasNot = false;
            this.indexOnlyFields = indexOnlyFields;
        }

        @Override
        public Object visit(ASTNENode node, Object data) {
            final String identifier = JexlASTHelper.getIdentifier(node);
            hasNot = true & indexOnlyFields.contains(identifier);
            return super.visit(node, data);
        }

        @Override
        public Object visit(ASTEQNode node, Object data) {

            hasNot |= JexlASTHelper.isDescendantOfNot(node);
            return super.visit(node, data);
        }

        public boolean hasNot() {
            return hasNot;
        }

    }
}
