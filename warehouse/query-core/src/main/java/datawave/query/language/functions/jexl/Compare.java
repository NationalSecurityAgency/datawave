package datawave.query.language.functions.jexl;

import com.google.common.collect.Sets;
import datawave.query.language.functions.QueryFunction;

import java.util.Set;

/**
 * The #COMPARE function is used to compare the values of two fields.
 * <p>
 * Function args are a field name, an operator, a mode, and another field name.
 * <p>
 * Lucene Example: #COMPARE(FIELD_A, &lt;, ANY, FIELD_B)
 * <p>
 * Jexl Example: filter:compare(FIELD_A, &lt;, ANY, FIELD_B)
 */
public class Compare extends AbstractEvaluationPhaseFunction {
    
    private final Set<String> validOpArgs = Sets.newHashSet("<", "<=", ">", ">=", "==", "=", "!=");
    private final Set<String> validModeArgs = Sets.newHashSet("ANY", "ALL");
    
    public Compare() {
        super("compare");
    }
    
    @Override
    public void validate() throws IllegalArgumentException {
        if (parameterList.size() != 4) {
            throw new IllegalArgumentException("#COMPARE function should have four args (fieldA, op, mode, fieldB)");
        }
        
        if (!validOpArgs.contains(parameterList.get(1))) {
            throw new IllegalArgumentException("#COMPARE function requires a valid op arg: " + validOpArgs);
        }
        
        if (!validModeArgs.contains(parameterList.get(2))) {
            throw new IllegalArgumentException("#COMPARE function requires a valid mode arg: " + validModeArgs);
        }
    }
    
    @Override
    public String toString() {
        // arg 0 - fieldA
        // arg 1 - op (LT, GT, LE, GE, EQ, NE)
        // arg 2 - op mode (ANY/ALL)
        // arg 3 - fieldB
        StringBuilder sb = new StringBuilder();
        sb.append("filter:compare(");
        sb.append(parameterList.get(0)).append(", '");
        sb.append(parameterList.get(1)).append("', '"); // operator and mode must be quoted for clean jexl parsing
        sb.append(parameterList.get(2)).append("', ");
        sb.append(parameterList.get(3)).append(")");
        return sb.toString();
    }
    
    @Override
    public QueryFunction duplicate() {
        return new Compare();
    }
}
