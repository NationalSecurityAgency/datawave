package datawave.webservice.query.util;

import com.google.protobuf.InvalidProtocolBufferException;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import java.util.HashSet;
import java.util.Set;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl.Parameter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

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
    
    public static Set<Parameter> parseParameters(final String parameters) {
        final Set<Parameter> params = new HashSet<>();
        if (null != parameters) {
            String[] param = parameters.split(PARAMETER_SEPARATOR);
            for (String yyy : param) {
                String[] parts = yyy.split(PARAMETER_NAME_VALUE_SEPARATOR);
                if (parts.length == 2) {
                    params.add(new Parameter(parts[0], parts[1]));
                }
            }
        }
        return params;
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
    
    public static String toParametersString(final Set<Parameter> parameters) {
        final StringBuilder params = new StringBuilder();
        if (null != parameters) {
            for (final Parameter param : parameters) {
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
