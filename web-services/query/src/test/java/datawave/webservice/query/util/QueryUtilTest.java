package datawave.webservice.query.util;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import com.google.protobuf.InvalidProtocolBufferException;

import datawave.core.query.util.QueryUtil;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.query.QueryImpl.Parameter;

public class QueryUtilTest {

    @Test
    public void testSerializationDeserialization() throws InvalidProtocolBufferException, ClassNotFoundException {
        QueryImpl q = new QueryImpl();
        q.setQueryLogicName("EventQuery");
        q.setExpirationDate(new Date());
        q.setId(UUID.randomUUID());
        q.setPagesize(10);
        q.setPageTimeout(-1);
        q.setQuery("FOO == BAR");
        q.setQueryName("test query");
        q.setQueryAuthorizations("ALL");
        q.setUserDN("some user");
        q.setOwner("some owner");
        q.setColumnVisibility("A&B");

        Set<Parameter> parameters = new HashSet<>();
        parameters.add(new Parameter("some param", "some value"));
        q.setParameters(parameters);

        Mutation m = QueryUtil.toMutation(q, new ColumnVisibility(q.getColumnVisibility()));

        Assert.assertEquals(1, m.getUpdates().size());

        byte[] value = m.getUpdates().get(0).getValue();
        Query q2 = QueryUtil.deserialize(QueryImpl.class.getName(), new Text("A&B"), new Value(value));

        Assert.assertEquals(q, q2);

    }

}
