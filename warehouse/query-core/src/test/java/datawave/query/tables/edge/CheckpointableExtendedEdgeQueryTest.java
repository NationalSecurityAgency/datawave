package datawave.query.tables.edge;

import datawave.query.edge.DefaultExtendedEdgeQueryLogic;
import datawave.query.edge.ExtendedEdgeQueryLogicTest;
import datawave.webservice.query.QueryImpl;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Set;

public class CheckpointableExtendedEdgeQueryTest extends ExtendedEdgeQueryLogicTest {

    @Override
    public DefaultExtendedEdgeQueryLogic runLogic(QueryImpl q, Set<Authorizations> auths) throws Exception {
        logic.setCheckpointable(true);
        return super.runLogic(q, auths);
    }

}
