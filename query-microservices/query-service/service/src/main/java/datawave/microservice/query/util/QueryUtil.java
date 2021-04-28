package datawave.microservice.query.util;

import com.google.protobuf.InvalidProtocolBufferException;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.Arrays;
import java.util.Set;

public class QueryUtil {
    public static final String PARAMETER_SEPARATOR = ";";
    public static final String PARAMETER_NAME_VALUE_SEPARATOR = ":";
    private static final String NULL_BYTE = "\u0000";
    
    public static String getQueryImplClassName(Key key) {
        String colq = key.getColumnQualifier().toString();
        String[] parts = colq.split(NULL_BYTE);
        if (parts.length == 2) {
            return parts[1];
        } else {
            // TODO: Need to update the existing Queries table to include the class name in the COLQ.
            throw new RuntimeException("Query impl class name not found in colq: " + colq);
        }
    }
    
    public static <T extends Query> T deserialize(String queryImplClassName, Text columnVisibility, Value value)
                    throws InvalidProtocolBufferException, ClassNotFoundException {
        @SuppressWarnings("unchecked")
        Class<T> queryClass = (Class<T>) Class.forName(queryImplClassName);
        byte[] b = value.get();
        Schema<T> schema = RuntimeSchema.getSchema(queryClass);
        T queryImpl = schema.newMessage();
        ProtobufIOUtil.mergeFrom(b, queryImpl, schema);
        queryImpl.setColumnVisibility(columnVisibility.toString());
        return queryImpl;
    }
    
    public static MultiValueMap<String,String> parseParameters(final String paramsString) {
        final MultiValueMap<String,String> parameters = new LinkedMultiValueMap<>();
        Arrays.stream(paramsString.split(PARAMETER_SEPARATOR)).map(x -> x.split(PARAMETER_NAME_VALUE_SEPARATOR)).filter(x -> x.length == 2)
                        .forEach(x -> parameters.add(x[0], x[1]));
        return parameters;
    }
    
    private static ThreadLocal<LinkedBuffer> BUFFER = ThreadLocal.withInitial(() -> LinkedBuffer.allocate(1024));
    
    public static <T extends Query> Mutation toMutation(T query, ColumnVisibility vis) {
        // Store by sid for backwards compatibility
        Mutation m = new Mutation(query.getOwner());
        try {
            @SuppressWarnings("unchecked")
            Schema<T> schema = (Schema<T>) RuntimeSchema.getSchema(query.getClass());
            byte[] bytes = ProtobufIOUtil.toByteArray(query, schema, BUFFER.get());
            m.put(query.getQueryName(), query.getId() + NULL_BYTE + query.getClass().getName(), vis, query.getExpirationDate().getTime(), new Value(bytes));
            return m;
        } finally {
            BUFFER.get().clear();
        }
    }
    
    public static String toParametersString(final Set<QueryImpl.Parameter> parameters) {
        final StringBuilder params = new StringBuilder();
        if (null != parameters) {
            for (final QueryImpl.Parameter param : parameters) {
                if (params.length() > 0) {
                    params.append(PARAMETER_SEPARATOR);
                }
                
                params.append(param.getParameterName());
                params.append(PARAMETER_NAME_VALUE_SEPARATOR);
                params.append(param.getParameterValue());
            }
        }
        return params.toString();
    }
}
