package datawave.query.language.functions.jexl;

import java.util.List;

import datawave.query.language.functions.QueryFunction;

import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

public abstract class JexlQueryFunction implements QueryFunction {

    protected String name = null;
    protected List<String> parameterList = null;
    protected int depth = -1;
    protected QueryNode parent = null;

    /** The last 7bits ascii character. */
    private static final char LAST_ASCII = 127;
    /** The first printable 7bits ascii character. */
    private static final char FIRST_ASCII = 32;
    private static final int UCHAR_LEN = 4;

    public JexlQueryFunction(String functionName, List<String> parameterList) {
        this.name = functionName;
        this.parameterList = parameterList;
    }

    @Override
    public void initialize(List<String> parameterList, int depth, QueryNode parent) throws IllegalArgumentException {
        this.parameterList = parameterList;
        this.depth = depth;
        this.parent = parent;
        validate();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public List<String> getParameterList() {
        return parameterList;
    }

    @Override
    public void setParameterList(List<String> parameterList) {
        this.parameterList = parameterList;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        boolean firstParam = true;
        sb.append(name);
        sb.append("(");
        for (String s : parameterList) {
            if (firstParam) {
                firstParam = false;
            } else {
                sb.append(", ");
            }
            sb.append(s);
        }
        sb.append(")");

        return sb.toString();
    }

    // escape ' (single quote) and \ (backslash) and wrap the str in ' (single quote)
    public String escapeString(String str) {
        if (str == null) {
            return null;
        }
        final int length = str.length();
        StringBuilder strb = new StringBuilder(length + 2);
        strb.append("'");
        for (int i = 0; i < length; ++i) {
            char c = str.charAt(i);
            switch (c) {
                case 0:
                    continue;
                case '\'':
                    strb.append("\\\'");
                    break;
                case '\\':
                    strb.append("\\\\");
                    break;
                default:
                    strb.append(c);
            }
        }
        strb.append("'");
        return strb.toString();
    }
}
