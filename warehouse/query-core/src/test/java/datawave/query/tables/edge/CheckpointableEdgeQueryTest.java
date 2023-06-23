package datawave.query.tables.edge;

import datawave.webservice.query.QueryImpl;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Set;

public class CheckpointableEdgeQueryTest extends EdgeQueryFunctionalTest {

    @Override
    public EdgeQueryLogic runLogic(QueryImpl q, Set<Authorizations> auths) throws Exception {
        logic.setCheckpointable(true);
        return super.runLogic(q, auths);
    }

}
