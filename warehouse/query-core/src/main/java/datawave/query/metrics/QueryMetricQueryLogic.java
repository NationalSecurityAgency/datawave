package datawave.query.metrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import javax.inject.Inject;

import datawave.query.language.parser.ParseException;
import datawave.query.tables.ShardQueryLogic;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.system.CallerPrincipal;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;

/**
 * Extension to the query logic that enforces the current user is equal to the USER field in the QueryMetrics <br>
 * <p>
 * QLF.xml entry: <br>
 *
 * <pre>
 * {@code
 *
 *  <bean id="QueryMetricsQuery" scope="prototype"  parent="BaseEventQuery" class="datawave.query.metrics.QueryMetricsQueryLogic">
 *      <property name="logicDescription" value="Query Metrics query for users" />
 *      <property name="includeHierarchyFields" value="false" />
 *      <property name="modelTableName" value="QueryMetrics_m" />
 *      <property name="modelName" value="NONE" />
 *      <property name="tableName" value="QueryMetrics_e" />
 *      <property name="metadataTableName" value="QueryMetrics_m" />
 *      <property name="indexTableName" value="QueryMetrics_i" />
 *      <property name="reverseIndexTableName" value="QueryMetrics_r" />
 *      <property name="maxValueExpansionThreshold" value="1500" />
 *      <property name="auditType" value="NONE" />
 *      <property name="collectQueryMetrics" value="false" />
 *  </bean>
 * }
 * </pre>
 *
 * <br>
 */
public class QueryMetricQueryLogic extends ShardQueryLogic {

    @Inject
    @CallerPrincipal
    private DatawavePrincipal callerPrincipal;

    private Collection<String> roles = null;

    public void setRolesSets(Collection<String> roleSets) {
        this.roles = roleSets;
    }

    public QueryMetricQueryLogic() {
        super();
    }

    public QueryMetricQueryLogic(QueryMetricQueryLogic other) {
        super(other);
        callerPrincipal = other.callerPrincipal;
        if (other.roles != null) {
            roles = new ArrayList<>();
            roles.addAll(other.roles);
        }
    }

    @Override
    public QueryMetricQueryLogic clone() {
        return new QueryMetricQueryLogic(this);
    }

    @Override
    public final GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set<Authorizations> auths) throws Exception {
        return super.initialize(client, settings, auths);
    }

    @Override
    public final String getJexlQueryString(Query settings) throws ParseException {

        if (null == this.roles) {
            this.roles = callerPrincipal.getPrimaryUser().getRoles();
        }

        String query = super.getJexlQueryString(settings);
        if (this.roles.contains("MetricsAdministrator")) {
            return query;
        }

        StringBuilder jexl = new StringBuilder();
        if (!query.isEmpty()) {
            jexl.append("(").append(query).append(")");
            jexl.append(" AND (USER == '").append(settings.getOwner()).append("')");
        } else {
            jexl.append("USER == '").append(settings.getOwner()).append("'");
        }
        return jexl.toString();
    }
}
