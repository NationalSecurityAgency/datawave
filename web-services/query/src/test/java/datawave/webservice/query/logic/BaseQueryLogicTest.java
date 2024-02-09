package datawave.webservice.query.logic;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Test;

import datawave.security.authorization.DatawavePrincipal;
import datawave.webservice.common.audit.Auditor;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;

public class BaseQueryLogicTest {

    @Test
    public void testCopyConstructor() {
        BaseQueryLogicImpl original = new BaseQueryLogicImpl();
        original.setLogicName("BaseQueryLogicImpl");
        original.setLogicDescription("Implementation of a BaseQueryLogic");
        original.setAuditType(Auditor.AuditType.PASSIVE);
        original.setDnResultLimits(Collections.singletonMap("dn=user", 100L));
        original.setSystemFromResultLimits(Collections.singletonMap("SYSTEM", 100L));
        original.setMaxResults(1000L);
        original.setMaxPageSize(100);
        original.setPageByteTrigger(123456L);
        original.setCollectQueryMetrics(false);
        original.setAuthorizedDNs(Collections.singleton("dn=authorized1"));
        original.setPrincipal(new DatawavePrincipal("user"));

        BaseQueryLogicImpl copy = new BaseQueryLogicImpl(original);
        assertEquals(original.getLogicName(), copy.getLogicName());
        assertEquals(original.getLogicDescription(), copy.getLogicDescription());
        assertEquals(original.getAuditType(), copy.getAuditType());
        assertEquals(original.getDnResultLimits(), copy.getDnResultLimits());
        assertEquals(original.getSystemFromResultLimits(), copy.getSystemFromResultLimits());
        assertEquals(original.getMaxResults(), copy.getMaxResults());
        assertEquals(original.getMaxPageSize(), copy.getMaxPageSize());
        assertEquals(original.getPageByteTrigger(), copy.getPageByteTrigger());
        assertEquals(original.getCollectQueryMetrics(), copy.getCollectQueryMetrics());
        assertEquals(original.getAuthorizedDNs(), copy.getAuthorizedDNs());
        assertEquals(original.getPrincipal(), copy.getPrincipal());
    }

    class BaseQueryLogicImpl extends BaseQueryLogic<Object> {

        public BaseQueryLogicImpl() {
            super();
        }

        public BaseQueryLogicImpl(BaseQueryLogicImpl other) {
            super(other);
        }

        @Override
        public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set<Authorizations> runtimeQueryAuthorizations) throws Exception {
            return null;
        }

        @Override
        public void setupQuery(GenericQueryConfiguration configuration) throws Exception {

        }

        @Override
        public Object clone() throws CloneNotSupportedException {
            return null;
        }

        @Override
        public AccumuloConnectionFactory.Priority getConnectionPriority() {
            return null;
        }

        @Override
        public QueryLogicTransformer getTransformer(Query settings) {
            return null;
        }

        @Override
        public Set<String> getOptionalQueryParameters() {
            return null;
        }

        @Override
        public Set<String> getRequiredQueryParameters() {
            return null;
        }

        @Override
        public Set<String> getExampleQueries() {
            return null;
        }
    }
}
