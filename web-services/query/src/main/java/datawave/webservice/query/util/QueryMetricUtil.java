package datawave.webservice.query.util;

import java.io.IOException;

import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetric;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;

//TODO: Need to replace this class with inject-able factory for instantiating BaseQueryMetric subclasses as needed
public class QueryMetricUtil {
    
    private static LinkedBuffer buffer = LinkedBuffer.allocate(1024);
    
    public static synchronized Mutation toMutation(BaseQueryMetric metric) throws IOException {
        try {
            byte[] bytes = ProtobufIOUtil.toByteArray((QueryMetric) metric, ((QueryMetric) metric).cachedSchema(), buffer);
            Mutation m = new Mutation(metric.getUser());
            m.put(metric.getQueryId(), metric.getQueryType() + "\u0000" + metric.getCreateDate().getTime(), new Value(bytes));
            return m;
        } finally {
            buffer.clear();
        }
    }
    
    public static BaseQueryMetric toMetric(Value value) throws IOException, ClassNotFoundException {
        byte[] b = value.get();
        QueryMetric m = new QueryMetric();
        ProtobufIOUtil.mergeFrom(b, m, m.cachedSchema());
        return m;
    }
}
