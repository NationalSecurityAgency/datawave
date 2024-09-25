package datawave.query.tables.edge;

import java.util.Set;

import org.apache.accumulo.core.security.Authorizations;

import datawave.microservice.query.QueryImpl;
import datawave.query.edge.DefaultExtendedEdgeQueryLogic;
import datawave.query.edge.ExtendedEdgeQueryLogicTest;

public class CheckpointableExtendedEdgeQueryTest extends ExtendedEdgeQueryLogicTest {

    @Override
    public DefaultExtendedEdgeQueryLogic runLogic(QueryImpl q, Set<Authorizations> auths) throws Exception {
        logic.setCheckpointable(true);
        return super.runLogic(q, auths);
    }

}
