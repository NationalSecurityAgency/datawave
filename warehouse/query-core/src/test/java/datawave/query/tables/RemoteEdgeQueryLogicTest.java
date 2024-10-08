package datawave.query.tables;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.remote.RemoteQueryService;
import datawave.microservice.query.QueryImpl;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.webservice.common.remote.RemoteHttpService;
import datawave.webservice.query.result.edge.DefaultEdge;
import datawave.webservice.query.result.edge.EdgeBase;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.DefaultEdgeQueryResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.VoidResponse;

public class RemoteEdgeQueryLogicTest {

    RemoteEdgeQueryLogic logic = new RemoteEdgeQueryLogic();

    @Before
    public void setup() {
        UUID uuid = UUID.randomUUID();
        GenericResponse<String> createResponse = new GenericResponse<String>();
        createResponse.setResult(uuid.toString());

        DefaultEdgeQueryResponse response1 = new DefaultEdgeQueryResponse();
        DefaultEdge edge1 = new DefaultEdge();
        edge1.setSource("source1");
        edge1.setSink("sink1");
        edge1.setCount(1L);
        edge1.setEdgeAttribute1Source("edgeAttr1Source1");
        edge1.setEdgeRelationship("edgeRel1");
        edge1.setEdgeAttribute2("edgeAttr21");
        edge1.setEdgeAttribute3("edgeAttr31");
        edge1.setDate("20230101");
        edge1.setLoadDate("20230101");
        edge1.setActivityDate("20230101");
        edge1.setEdgeType("type1");
        edge1.setColumnVisibility("PUBLIC");
        response1.setEdges(Collections.singletonList(edge1));
        response1.setTotalResults(1L);

        DefaultEdgeQueryResponse response2 = new DefaultEdgeQueryResponse();
        DefaultEdge edge2 = new DefaultEdge();
        edge2.setSource("source2");
        edge2.setSink("sink2");
        edge2.setCount(1L);
        edge2.setEdgeAttribute1Source("edgeAttr1Source2");
        edge2.setEdgeRelationship("edgeRel2");
        edge2.setEdgeAttribute2("edgeAttr22");
        edge2.setEdgeAttribute3("edgeAttr32");
        edge2.setDate("20230101");
        edge2.setLoadDate("20230101");
        edge2.setActivityDate("20230101");
        edge2.setEdgeType("type2");
        edge2.setColumnVisibility("PUBLIC");
        response2.setEdges(Collections.singletonList(edge1));
        response2.setTotalResults(1L);

        DefaultEdgeQueryResponse response3 = new DefaultEdgeQueryResponse();
        response3.setTotalResults(0L);

        // create a remote Edge query logic that has our own remote query service behind it
        logic.setRemoteQueryService(new TestRemoteQueryService(createResponse, response1, response2, response3));
        logic.setRemoteQueryLogic("TestQuery");
    }

    @Test
    public void testRemoteQuery() throws Exception {
        GenericQueryConfiguration config = logic.initialize(null, new QueryImpl(), null);
        logic.setupQuery(config);
        Iterator<EdgeBase> t = logic.iterator();
        List<EdgeBase> Edges = new ArrayList();
        while (t.hasNext()) {
            Edges.add(t.next());
        }
        assertEquals(2, Edges.size());
    }

    @Test
    public void testProxiedHeaders() throws Exception {
        SubjectIssuerDNPair userDN = SubjectIssuerDNPair.of("userDN", "issuerDN");
        SubjectIssuerDNPair p1dn = SubjectIssuerDNPair.of("entity1UserDN", "entity1IssuerDN");
        SubjectIssuerDNPair p2dn = SubjectIssuerDNPair.of("entity2UserDN", "entity2IssuerDN");
        SubjectIssuerDNPair p3dn = SubjectIssuerDNPair.of("entity3UserDN", "entity3IssuerDN");

        DatawaveUser user = new DatawaveUser(userDN, DatawaveUser.UserType.USER, Sets.newHashSet("A", "C", "D"), null, null, System.currentTimeMillis());
        DatawaveUser p1 = new DatawaveUser(p1dn, DatawaveUser.UserType.SERVER, Sets.newHashSet("A", "B", "E"), null, null, System.currentTimeMillis());
        DatawaveUser p2 = new DatawaveUser(p2dn, DatawaveUser.UserType.SERVER, Sets.newHashSet("A", "F", "G"), null, null, System.currentTimeMillis());
        DatawaveUser p3 = new DatawaveUser(p3dn, DatawaveUser.UserType.SERVER, Sets.newHashSet("A", "B", "G"), null, null, System.currentTimeMillis());

        DatawavePrincipal proxiedUserPrincipal = new DatawavePrincipal(Lists.newArrayList(user, p1, p2));
        DatawavePrincipal proxiedServerPrincipal1 = new DatawavePrincipal(Lists.newArrayList(p3, p1));
        DatawavePrincipal proxiedServerPrincipal2 = new DatawavePrincipal(Lists.newArrayList(p2, p3, p1));

        Assert.assertEquals("<userdn><entity1userdn><entity2userdn>", RemoteHttpService.getProxiedEntities(proxiedUserPrincipal));
        Assert.assertEquals("<entity3userdn><entity1userdn>", RemoteHttpService.getProxiedEntities(proxiedServerPrincipal1));
        Assert.assertEquals("<entity2userdn><entity3userdn><entity1userdn>", RemoteHttpService.getProxiedEntities(proxiedServerPrincipal2));

        Assert.assertEquals("<issuerdn><entity1issuerdn><entity2issuerdn>", RemoteHttpService.getProxiedIssuers(proxiedUserPrincipal));
        Assert.assertEquals("<entity3issuerdn><entity1issuerdn>", RemoteHttpService.getProxiedIssuers(proxiedServerPrincipal1));
        Assert.assertEquals("<entity2issuerdn><entity3issuerdn><entity1issuerdn>", RemoteHttpService.getProxiedIssuers(proxiedServerPrincipal2));
    }

    public static class TestRemoteQueryService implements RemoteQueryService {
        GenericResponse<String> createResponse;
        LinkedList<BaseQueryResponse> nextResponses;

        public TestRemoteQueryService(GenericResponse<String> createResponse, BaseQueryResponse response1, BaseQueryResponse response2,
                        BaseQueryResponse response3) {
            this.createResponse = createResponse;
            this.nextResponses = new LinkedList<>();
            nextResponses.add(response1);
            nextResponses.add(response2);
            nextResponses.add(response3);
        }

        @Override
        public GenericResponse<String> createQuery(String queryLogicName, Map<String,List<String>> queryParameters, ProxiedUserDetails callerObject) {
            return createResponse;
        }

        @Override
        public void setNextQueryResponseClass(Class<? extends BaseQueryResponse> nextQueryResponseClass) {
            // noop
        }

        @Override
        public BaseQueryResponse next(String id, ProxiedUserDetails callerObject) {
            return nextResponses.poll();
        }

        @Override
        public VoidResponse close(String id, ProxiedUserDetails callerObject) {
            return new VoidResponse();
        }

        @Override
        public GenericResponse<String> planQuery(String queryLogicName, Map<String,List<String>> queryParameters, ProxiedUserDetails callerObject) {
            throw new UnsupportedOperationException();
        }

        @Override
        public GenericResponse<String> planQuery(String id, ProxiedUserDetails callerObject) {
            throw new UnsupportedOperationException();
        }

        @Override
        public URI getQueryMetricsURI(String id) {
            try {
                return new URI("https://localhost:8443/DataWave/Query/Metrics/id/" + id);
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
