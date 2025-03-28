package datawave.security.authorization.remote;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.wildfly.common.Assert;

import com.google.common.collect.HashMultimap;

import datawave.microservice.query.Query;
import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.ConditionalRemoteUserOperations;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.authorization.UserOperations;
import datawave.user.AuthorizationsListBase;
import datawave.user.DefaultAuthorizationsList;
import datawave.webservice.dictionary.data.DataDictionaryBase;
import datawave.webservice.dictionary.data.DescriptionBase;
import datawave.webservice.dictionary.data.FieldsBase;
import datawave.webservice.metadata.MetadataFieldBase;
import datawave.webservice.query.cachedresults.CacheableQueryRow;
import datawave.webservice.query.result.EdgeQueryResponseBase;
import datawave.webservice.query.result.edge.EdgeBase;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FacetsBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.FieldCardinalityBase;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.response.objects.KeyBase;
import datawave.webservice.result.EventQueryResponseBase;
import datawave.webservice.result.FacetQueryResponseBase;
import datawave.webservice.result.GenericResponse;

public class ConditionalRemoteUserOperationsTest {

    private static class MockRemoteUserOperations implements UserOperations {

        boolean invoked = false;

        @Override
        public AuthorizationsListBase listEffectiveAuthorizations(ProxiedUserDetails callerObject) throws AuthorizationException {
            invoked = true;
            return new DefaultAuthorizationsList();
        }

        @Override
        public GenericResponse<String> flushCachedCredentials(ProxiedUserDetails callerObject) {
            invoked = true;
            return new GenericResponse<>();
        }
    }

    @Test
    public void testConditional() throws AuthorizationException {
        MockRemoteUserOperations testOperations = new MockRemoteUserOperations();
        ConditionalRemoteUserOperations testObj = new ConditionalRemoteUserOperations();
        testObj.setDelegate(testOperations);
        testObj.setAuthorizationsListBaseSupplier(() -> new MockResponseObjectFactory().getAuthorizationsList());
        testObj.setCondition(a -> a.getProxiedUsers().size() == 1);

        List<DatawaveUser> users = new ArrayList<>();
        users.add(new DatawaveUser(SubjectIssuerDNPair.of("userdn", "issuerdn"), DatawaveUser.UserType.USER, Arrays.asList(new String[] {"auth1", "auth2"}),
                        new ArrayList<>(), HashMultimap.create(), System.currentTimeMillis()));
        DatawavePrincipal principal = new DatawavePrincipal(users, System.currentTimeMillis());

        testOperations.invoked = false;
        testObj.listEffectiveAuthorizations(principal);
        Assert.assertTrue(testOperations.invoked);
        testOperations.invoked = false;
        testObj.flushCachedCredentials(principal);
        Assert.assertTrue(testOperations.invoked);
        testOperations.invoked = false;

        users.add(new DatawaveUser(SubjectIssuerDNPair.of("userdn", "issuerdn"), DatawaveUser.UserType.SERVER, Arrays.asList(new String[] {"auth2", "auth3"}),
                        new ArrayList<>(), HashMultimap.create(), System.currentTimeMillis()));
        principal = new DatawavePrincipal(users, System.currentTimeMillis());

        testObj.listEffectiveAuthorizations(principal);
        Assert.assertFalse(testOperations.invoked);
        testObj.flushCachedCredentials(principal);
        Assert.assertFalse(testOperations.invoked);
    }

    public static class MockResponseObjectFactory extends ResponseObjectFactory {

        @Override
        public EventBase getEvent() {
            return null;
        }

        @Override
        public FieldBase getField() {
            return null;
        }

        @Override
        public EventQueryResponseBase getEventQueryResponse() {
            return null;
        }

        @Override
        public CacheableQueryRow getCacheableQueryRow() {
            return null;
        }

        @Override
        public EdgeBase getEdge() {
            return null;
        }

        @Override
        public EdgeQueryResponseBase getEdgeQueryResponse() {
            return null;
        }

        @Override
        public FacetQueryResponseBase getFacetQueryResponse() {
            return null;
        }

        @Override
        public FacetsBase getFacets() {
            return null;
        }

        @Override
        public FieldCardinalityBase getFieldCardinality() {
            return null;
        }

        @Override
        public KeyBase getKey() {
            return null;
        }

        @Override
        public AuthorizationsListBase getAuthorizationsList() {
            return new DefaultAuthorizationsList();
        }

        @Override
        public Query getQueryImpl() {
            return null;
        }

        @Override
        public DataDictionaryBase getDataDictionary() {
            return null;
        }

        @Override
        public FieldsBase getFields() {
            return null;
        }

        @Override
        public DescriptionBase getDescription() {
            return null;
        }

        @Override
        public MetadataFieldBase getMetadataField() {
            return null;
        }
    }

}
