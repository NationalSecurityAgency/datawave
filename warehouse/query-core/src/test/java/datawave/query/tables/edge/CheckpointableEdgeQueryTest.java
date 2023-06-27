package datawave.query.tables.edge;

import java.util.Set;

import org.apache.accumulo.core.security.Authorizations;

import datawave.webservice.query.QueryImpl;

public class CheckpointableEdgeQueryTest extends EdgeQueryFunctionalTest {

    @Override
    public EdgeQueryLogic runLogic(QueryImpl q, Set<Authorizations> auths) throws Exception {
        logic.setCheckpointable(true);
        return super.runLogic(q, auths);
    }

}
