package datawave.query.language.functions.jexl;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import datawave.query.language.functions.QueryFunction;
import datawave.query.language.parser.ParseException;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.resteasy.util.DateFormatter;
import datawave.webservice.query.QueryImpl.Parameter;

import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

public class ChainStart extends JexlQueryFunction {
    
    // chainstart(EventQuery, 'field1:term1 AND field2:term3', 20140101, 20140731, 'params')
    // chain('a=field4, b=[field6|field7]', EventQuery, 'field4:${field4} AND fields:${field5}', 20140101, 20140731, params)
    
    private static final String PARAMETER_SEPARATOR = ";";
    private static final String PARAMETER_NAME_VALUE_SEPARATOR = ":";
    private String begin = null;
    private String end = null;
    private String queryLogic = null;
    private String query = null;
    private String params = null;
    private Map<String,Parameter> parameterMap = new HashMap<>();
    
    public ChainStart() {
        super("chainstart", new ArrayList<>());
    }
    
    @Override
    public void initialize(List<String> parameterList, int depth, QueryNode parent) throws IllegalArgumentException {
        super.initialize(parameterList, depth, parent);
    }
    
    @Override
    public void validate() throws IllegalArgumentException {
        
        if (this.parameterList.size() < 4 || this.parameterList.size() > 5) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", this.name));
            throw new IllegalArgumentException(qe);
        }
        queryLogic = parameterList.get(0);
        query = parameterList.get(1);
        begin = parameterList.get(2);
        end = parameterList.get(3);
        if (parameterList.size() == 5) {
            params = parameterList.get(4);
        }
        
        LuceneToJexlQueryParser parser = new LuceneToJexlQueryParser();
        try {
            parser.parse(query);
        } catch (ParseException e) {
            throw new IllegalArgumentException("invalid value for query argument to function: " + this.name + " (" + query + ")", e);
        }
        
        DateFormatUtil dateFormatter = new DateFormatUtil();
        try {
            dateFormatter.fromString(begin);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("invalid value for begin argument to function: " + this.name + " (" + begin + ")", e);
        }
        try {
            dateFormatter.fromString(end);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("invalid value for end argument to function: " + this.name + " (" + end + ")", e);
        }
        if (params != null) {
            try {
                parameterMap = parseParameters(params);
            } catch (RuntimeException e) {
                throw new IllegalArgumentException("invalid value for params argument to function: " + this.name + " (" + params + ")", e);
            }
        }
    }
    
    public String getBegin() {
        return begin;
    }
    
    public String getEnd() {
        return end;
    }
    
    public String getQueryLogic() {
        return queryLogic;
    }
    
    public String getQuery() {
        return query;
    }
    
    public String getParams() {
        return params;
    }
    
    private Map<String,Parameter> parseParameters(String params) {
        
        Map<String,Parameter> parameterMap = new HashMap<>();
        if (null != params) {
            String[] param = params.split(PARAMETER_SEPARATOR);
            for (String yyy : param) {
                String[] parts = yyy.split(PARAMETER_NAME_VALUE_SEPARATOR);
                if (parts.length == 2) {
                    parameterMap.put(parts[0], new Parameter(parts[0], parts[1]));
                }
            }
        }
        return parameterMap;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        
        sb.append("#");
        sb.append(this.getName().toUpperCase());
        sb.append("(");
        boolean first = true;
        for (String p : parameterList) {
            if (!first) {
                sb.append(", ");
            }
            first = false;
            sb.append(escapeString(p));
        }
        sb.append(")");
        return sb.toString();
    }
    
    @Override
    public QueryFunction duplicate() {
        return new ChainStart();
    }
    
    public Map<String,Parameter> getParameterMap() {
        return parameterMap;
    }
}
