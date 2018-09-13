package datawave.query.language.functions.jexl;

import java.util.ArrayList;
import java.util.List;

import datawave.query.language.functions.QueryFunction;

/**
 */
public class Jexl extends JexlQueryFunction {
    
    public Jexl() {
        super("jexl", new ArrayList<>());
    }
    
    @Override
    public void validate() throws IllegalArgumentException {}
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        List<String> params = getParameterList();
        for (String param : params) {
            sb.append(param);
        }
        return sb.toString();
    }
    
    @Override
    public QueryFunction duplicate() {
        return new Jexl();
    }
    
}
