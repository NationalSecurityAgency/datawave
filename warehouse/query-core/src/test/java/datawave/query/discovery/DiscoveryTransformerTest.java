package datawave.query.discovery;

import org.apache.hadoop.io.MapWritable;
import org.junit.jupiter.api.Test;

import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.microservice.query.Query;
import datawave.query.model.QueryModel;
import datawave.query.tables.ShardIndexQueryTable;
import datawave.webservice.query.result.event.EventBase;

public class DiscoveryTransformerTest {

    @Test
    public void testTermsOnlyDiscoveredThing() {

        // TODO: Work-in-progress
        BaseQueryLogic<DiscoveredThing> logic = null;
        Query settings = null;
        QueryModel qm = null;
        DiscoveryTransformer dt = new DiscoveryTransformer(logic, settings, qm);

        ShardIndexQueryTable siqt = new ShardIndexQueryTable();
        DiscoveredThing thing = new DiscoveredThing("onyx", "", "", "", "FOO", 0L, new MapWritable());

        QueryLogicTransformer<DiscoveredThing,EventBase> transformer = logic.getTransformer(settings);

        EventBase eb = transformer.transform(thing);

    }

    @Test
    public void testSingleDiscoveredThing() {
        // TODO: Work-in-progress
        // DiscoveryTransformer dt = new DiscoveryTransformer();
        DiscoveredThing thing = new DiscoveredThing("bbc", "NETWORK", "csv", "20130101", "FOO", 240L, new MapWritable());
        // EventBase eb = dt.transform(thing);

    }
}
