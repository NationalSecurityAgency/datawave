package datawave.query.tables.edge;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.accumulo.core.security.Authorizations;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.QueryLogicFactory;
import datawave.microservice.query.QueryImpl;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.exception.UnauthorizedQueryException;

@RunWith(Arquillian.class)
public class EdgeQueryFunctionalTest extends BaseEdgeQueryTest {
    protected EdgeQueryLogic logic;

    @Inject
    protected ApplicationContext applicationContext;

    protected QueryLogicFactory factory = new TestQueryLogicFactory();

    @Before
    public void setup() throws Exception {
        logic = (EdgeQueryLogic) createLogic();
    }

    public QueryLogic<?> createLogic() throws Exception {
        return factory.getQueryLogic("RewriteEdgeQuery");
    }

    /*
     * NOTE: If you're trying to debug within your IDE's debugger and you're getting Spring errors related to EdgeModelContext.xml or NoClassDefFound related to
     * EdgeModelAware.java, then the easiest way to resolve is to edit the JUnit run config in your IDE to pass the following JVM argument:
     *
     * -Dedge.model.context.path=file:///absolute/path/to/query-core/src/test/resources/EdgeModelContext.xml
     *
     * This will override EdgeModelAware's default context loading behavior, and avoid the pitfalls of failed classpath resolution and/or non-interpolated
     * resource files while debugging within Eclipse...all the stuff that works seamlessly when running Maven from the CL
     */

    @Deployment
    public static JavaArchive createDeployment() throws Exception {
        return ShrinkWrap.create(JavaArchive.class)
                        .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "datawave.query", "datawave.webservice.query.result.event",
                                        "datawave.core.query.result.event")
                        .deleteClass(DefaultEdgeEventQueryLogic.class).deleteClass(RemoteEdgeDictionary.class)
                        .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class)
                        .addAsManifestResource(new StringAsset(
                                        "<alternatives>" + "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>" + "</alternatives>"),
                                        "beans.xml");
    }

    public EdgeQueryLogic runLogic(QueryImpl q, Set<Authorizations> auths) throws Exception {
        GenericQueryConfiguration config = logic.initialize(client, q, auths);
        logic.setupQuery(config);
        return logic;
    }

    @Test
    public void testSingleQuery() throws Exception {
        QueryImpl q = configQuery("(SOURCE == 'PLUTO')", auths);
        EdgeQueryLogic logic = runLogic(q, auths);

        List<String> expected = new ArrayList<>();
        expected.add("pluto%00;charon AdjacentCelestialBodies/FROM-TO:20150713/NEW_HORIZONS-NEW_HORIZONS [C]");
        expected.add("pluto%00;neptune AdjacentDwarfPlanets/FROM-TO:20150713/NEW_HORIZONS-NEW_HORIZONS [C]");
        expected.add("pluto%00;neptune AdjacentPlanets/TO-FROM:20150713/NEW_HORIZONS-NEW_HORIZONS [C]");
        expected.add("pluto STATS/ACTIVITY/DwarfPlanets/TO:20150713/NEW_HORIZONS [D]");

        compareResults(logic, factory, expected);
    }

    @Test
    public void testSingleQueryMixedCase() throws Exception {
        QueryImpl q = configQuery("(SOURCE == 'pLUTO')", auths);
        EdgeQueryLogic logic = runLogic(q, auths);

        List<String> expected = new ArrayList<>();
        expected.add("pluto%00;charon AdjacentCelestialBodies/FROM-TO:20150713/NEW_HORIZONS-NEW_HORIZONS [C]");
        expected.add("pluto%00;neptune AdjacentDwarfPlanets/FROM-TO:20150713/NEW_HORIZONS-NEW_HORIZONS [C]");
        expected.add("pluto%00;neptune AdjacentPlanets/TO-FROM:20150713/NEW_HORIZONS-NEW_HORIZONS [C]");
        expected.add("pluto STATS/ACTIVITY/DwarfPlanets/TO:20150713/NEW_HORIZONS [D]");

        compareResults(logic, factory, expected);
    }

    @Test
    public void testSingleQueryLowerCase() throws Exception {
        QueryImpl q = configQuery("(SOURCE == 'pluto')", auths);
        EdgeQueryLogic logic = runLogic(q, auths);

        List<String> expected = new ArrayList<>();
        expected.add("pluto%00;charon AdjacentCelestialBodies/FROM-TO:20150713/NEW_HORIZONS-NEW_HORIZONS [C]");
        expected.add("pluto%00;neptune AdjacentDwarfPlanets/FROM-TO:20150713/NEW_HORIZONS-NEW_HORIZONS [C]");
        expected.add("pluto%00;neptune AdjacentPlanets/TO-FROM:20150713/NEW_HORIZONS-NEW_HORIZONS [C]");
        expected.add("pluto STATS/ACTIVITY/DwarfPlanets/TO:20150713/NEW_HORIZONS [D]");

        compareResults(logic, factory, expected);
    }

    @Test
    public void testSinglePatternQuery() throws Exception {
        QueryImpl q = configQuery("(SOURCE =~ 'E.*')", auths);
        EdgeQueryLogic logic = runLogic(q, auths);

        List<String> expected = new ArrayList<>();
        expected.add("earth%00;moon AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("earth%00;venus AdjacentPlanets/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("eris%00;dysnomia AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [B]");
        expected.add("earth%00;mars AdjacentPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("earth STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");
        expected.add("eris STATS/ACTIVITY/DwarfPlanets/TO:20150713/COSMOS_DATA [D]");

        compareResults(logic, factory, expected);
    }

    @Test
    public void testANDQuery() throws Exception {
        QueryImpl q = configQuery("(SOURCE == 'EARTH') && (SINK == 'MOON')", auths);
        EdgeQueryLogic logic = runLogic(q, auths);

        List<String> expected = new ArrayList<>();
        expected.add("earth%00;moon AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("earth STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");
        compareResults(logic, factory, expected);
    }

    @Test
    public void testANDQueryWithPatterns() throws Exception {
        QueryImpl q = configQuery("(SOURCE =~ 'E.*') && (SINK =~ '.*S' )", auths);
        EdgeQueryLogic logic = runLogic(q, auths);

        List<String> expected = new ArrayList<>();
        expected.add("earth%00;mars AdjacentPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("earth%00;venus AdjacentPlanets/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("earth STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");
        expected.add("eris STATS/ACTIVITY/DwarfPlanets/TO:20150713/COSMOS_DATA [D]");
        compareResults(logic, factory, expected);
    }

    @Test
    public void testORQuery() throws Exception {
        QueryImpl q = configQuery("(SOURCE == 'EARTH' || SOURCE == 'PLUTO')", auths);
        EdgeQueryLogic logic = runLogic(q, auths);

        List<String> expected = new ArrayList<>();
        expected.add("earth%00;mars AdjacentPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("earth%00;moon AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("earth%00;venus AdjacentPlanets/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("earth STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");

        expected.add("pluto%00;charon AdjacentCelestialBodies/FROM-TO:20150713/NEW_HORIZONS-NEW_HORIZONS [C]");
        expected.add("pluto%00;neptune AdjacentDwarfPlanets/FROM-TO:20150713/NEW_HORIZONS-NEW_HORIZONS [C]");
        expected.add("pluto%00;neptune AdjacentPlanets/TO-FROM:20150713/NEW_HORIZONS-NEW_HORIZONS [C]");
        expected.add("pluto STATS/ACTIVITY/DwarfPlanets/TO:20150713/NEW_HORIZONS [D]");

        compareResults(logic, factory, expected);
    }

    @Test
    public void testORQueryWithPattern() throws Exception {
        QueryImpl q = configQuery("(SOURCE =~ 'E.*' || SOURCE == 'PLUTO')", auths);
        EdgeQueryLogic logic = runLogic(q, auths);

        List<String> expected = new ArrayList<>();
        expected.add("earth%00;mars AdjacentPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("earth%00;venus AdjacentPlanets/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("earth%00;moon AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("pluto%00;charon AdjacentCelestialBodies/FROM-TO:20150713/NEW_HORIZONS-NEW_HORIZONS [C]");
        expected.add("pluto STATS/ACTIVITY/DwarfPlanets/TO:20150713/NEW_HORIZONS [D]");
        expected.add("pluto%00;neptune AdjacentDwarfPlanets/FROM-TO:20150713/NEW_HORIZONS-NEW_HORIZONS [C]");
        expected.add("pluto%00;neptune AdjacentPlanets/TO-FROM:20150713/NEW_HORIZONS-NEW_HORIZONS [C]");
        expected.add("earth STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");
        expected.add("eris STATS/ACTIVITY/DwarfPlanets/TO:20150713/COSMOS_DATA [D]");
        expected.add("eris%00;dysnomia AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [B]");

        compareResults(logic, factory, expected);
    }

    @Test
    public void testCombinationQuery() throws Exception {
        QueryImpl q = configQuery("(SOURCE == 'EARTH' || SOURCE == 'ASTEROID_BELT') && (SINK == 'MARS')", auths);
        EdgeQueryLogic logic = runLogic(q, auths);

        List<String> expected = new ArrayList<>();
        expected.add("earth%00;mars AdjacentPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("earth STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");
        expected.add("asteroid_belt%00;mars AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");

        compareResults(logic, factory, expected);
    }

    @Test
    public void testCombinationQueryWithPattern() throws Exception {
        QueryImpl q = configQuery("(SOURCE =~ 'E.*' || SOURCE == 'ASTEROID_BELT') && (SINK == 'MARS')", auths);
        EdgeQueryLogic logic = runLogic(q, auths);

        List<String> expected = new ArrayList<>();
        expected.add("earth%00;mars AdjacentPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("earth STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");
        expected.add("asteroid_belt%00;mars AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("eris STATS/ACTIVITY/DwarfPlanets/TO:20150713/COSMOS_DATA [D]");

        compareResults(logic, factory, expected);
    }

    @Test
    public void testComplexQuery1() throws Exception {
        QueryImpl q = configQuery(
                        "(SOURCE == 'EARTH' || SOURCE == 'PLUTO' || SOURCE == 'MARS' || SOURCE == 'CERES' || SOURCE == 'ASTEROID_BELT') && RELATION == 'FROM-TO' && TYPE == 'AdjacentCelestialBodies'",
                        auths);
        EdgeQueryLogic logic = runLogic(q, auths);

        List<String> expected = new ArrayList<>();
        expected.add("earth%00;moon AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("pluto%00;charon AdjacentCelestialBodies/FROM-TO:20150713/NEW_HORIZONS-NEW_HORIZONS [C]");

        expected.add("mars%00;ceres AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [B]");
        expected.add("ceres%00;jupiter AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("asteroid_belt%00;mars AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("asteroid_belt%00;ceres AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("asteroid_belt%00;jupiter AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");

        compareResults(logic, factory, expected);
    }

    @Test
    public void testComplexQueryWithPatterns() throws Exception {

        QueryImpl q = configQuery(
                        "(SOURCE =~ 'E.*' || SOURCE == 'PLUTO' || SOURCE =~ 'M.*' || SOURCE == 'CERES' || SOURCE == 'ASTEROID_BELT') && RELATION == 'FROM-TO' && TYPE =~ 'Adjacent.*'",
                        auths);
        EdgeQueryLogic logic = runLogic(q, auths);

        List<String> expected = new ArrayList<>();
        expected.add("earth%00;mars AdjacentPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("earth%00;moon AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("eris%00;dysnomia AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [B]");
        expected.add("pluto%00;charon AdjacentCelestialBodies/FROM-TO:20150713/NEW_HORIZONS-NEW_HORIZONS [C]");
        expected.add("pluto%00;neptune AdjacentDwarfPlanets/FROM-TO:20150713/NEW_HORIZONS-NEW_HORIZONS [C]");
        expected.add("mars%00;ceres AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [B]");
        expected.add("ceres%00;jupiter AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("ceres%00;jupiter AdjacentDwarfPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [B]");
        expected.add("ceres%00;mars AdjacentDwarfPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [B]");
        expected.add("asteroid_belt%00;mars AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("asteroid_belt%00;ceres AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("asteroid_belt%00;jupiter AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mars%00;jupiter AdjacentPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mercury%00;venus AdjacentPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");

        compareResults(logic, factory, expected);
    }

    @Test
    public void testSourceAndSinkPatterns() throws Exception {
        QueryImpl q = configQuery("(SOURCE =~ 'E.*' || SOURCE =~ 'P.*') && (SINK =~ 'M.*' || SINK =~ 'C.*')", auths);
        EdgeQueryLogic logic = runLogic(q, auths);

        List<String> expected = new ArrayList<>();
        expected.add("earth%00;mars AdjacentPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("earth%00;moon AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("pluto%00;charon AdjacentCelestialBodies/FROM-TO:20150713/NEW_HORIZONS-NEW_HORIZONS [C]");
        expected.add("earth STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");
        expected.add("pluto STATS/ACTIVITY/DwarfPlanets/TO:20150713/NEW_HORIZONS [D]");
        expected.add("eris STATS/ACTIVITY/DwarfPlanets/TO:20150713/COSMOS_DATA [D]");

        compareResults(logic, factory, expected);
    }

    @Test
    public void testTypeQuery() throws Exception {
        QueryImpl q = configQuery("SOURCE == 'JUPITER' && TYPE == 'AdjacentDwarfPlanets'", auths);
        EdgeQueryLogic logic = runLogic(q, auths);

        List<String> expected = new ArrayList<>();
        expected.add("jupiter%00;ceres AdjacentDwarfPlanets/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [B]");

        compareResults(logic, factory, expected);
    }

    @Test
    public void testPatternWithType() throws Exception {
        QueryImpl q = configQuery("(SOURCE =~ 'M.*') && TYPE == 'AdjacentPlanets' ", auths);
        EdgeQueryLogic logic = runLogic(q, auths);

        List<String> expected = new ArrayList<>();
        expected.add("mars%00;earth AdjacentPlanets/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mars%00;jupiter AdjacentPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mercury%00;venus AdjacentPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        compareResults(logic, factory, expected);
    }

    @Test
    public void testPatternWithTypeAndRelation() throws Exception {
        QueryImpl q = configQuery("(SOURCE =~ 'M.*') && TYPE == 'AdjacentPlanets' && RELATION ==  'TO-FROM' ", auths);
        EdgeQueryLogic logic = runLogic(q, auths);

        List<String> expected = new ArrayList<>();
        expected.add("mars%00;earth AdjacentPlanets/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [A]");

        compareResults(logic, factory, expected);
    }

    @Test
    public void testPatternWithRelation() throws Exception {
        QueryImpl q = configQuery("(SOURCE =~ 'M.*') && RELATION ==  'TO-FROM' ", auths);
        EdgeQueryLogic logic = runLogic(q, auths);

        List<String> expected = new ArrayList<>();
        expected.add("mars%00;earth AdjacentPlanets/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mercury%00;sun AdjacentCelestialBodies/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("moon%00;earth AdjacentCelestialBodies/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mars%00;asteroid_belt AdjacentCelestialBodies/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mars%00;ceres AdjacentDwarfPlanets/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [B]");
        expected.add("mars STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");
        expected.add("mercury STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");
        compareResults(logic, factory, expected);
    }

    @Test
    public void testPatternWithLeadingWildcard() throws Exception {
        QueryImpl q = configQuery("(SOURCE =~ 'M.*') && SINK =~ '.*S' && (RELATION ==  'FROM-TO' || RELATION == 'TO-FROM')", auths);
        EdgeQueryLogic logic = runLogic(q, auths);

        List<String> expected = new ArrayList<>();
        expected.add("mars%00;ceres AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [B]");
        expected.add("mars STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");
        expected.add("mercury STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");
        expected.add("mercury%00;venus AdjacentPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mars%00;ceres AdjacentDwarfPlanets/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [B]");
        compareResults(logic, factory, expected);
    }

    @Test
    public void testLiteralWithPatternAndRelation() throws Exception {
        QueryImpl q = configQuery(
                        "(( SOURCE == 'CERES') && ( SINK =~ 'JUP.*' ) && ( RELATION ==  'FROM-TO' || RELATION == 'TO-FROM') ) || ( (SOURCE =~ 'E.*') && (SINK == 'MARS' ) ) ",
                        auths);
        EdgeQueryLogic logic = runLogic(q, auths);

        List<String> expected = new ArrayList<>();
        expected.add("ceres%00;jupiter AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("ceres%00;jupiter AdjacentDwarfPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [B]");
        expected.add("earth%00;mars AdjacentPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("ceres STATS/ACTIVITY/DwarfPlanets/TO:20150713/COSMOS_DATA [D]");
        expected.add("earth STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");
        expected.add("eris STATS/ACTIVITY/DwarfPlanets/TO:20150713/COSMOS_DATA [D]");
        compareResults(logic, factory, expected);
    }

    @Test
    public void testNOT() throws Exception {
        QueryImpl q = configQuery("SOURCE == 'MARS' && SINK != 'CERES'", auths);

        EdgeQueryLogic logic = runLogic(q, auths);

        List<String> expected = new ArrayList<>();
        expected.add("mars%00;jupiter AdjacentPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mars%00;earth AdjacentPlanets/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mars STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");
        expected.add("mars%00;asteroid_belt AdjacentCelestialBodies/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        compareResults(logic, factory, expected);
    }

    @Test
    public void testPatternWithNot() throws Exception {
        QueryImpl q = configQuery("SOURCE =~ 'M.*' && SINK != 'CERES'", auths);

        EdgeQueryLogic logic = runLogic(q, auths);

        List<String> expected = new ArrayList<>();
        expected.add("mars%00;jupiter AdjacentPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mars%00;earth AdjacentPlanets/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mars STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");
        expected.add("mars%00;asteroid_belt AdjacentCelestialBodies/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mercury%00;sun AdjacentCelestialBodies/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mercury STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");
        expected.add("moon%00;earth AdjacentCelestialBodies/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mercury%00;venus AdjacentPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        compareResults(logic, factory, expected);
    }

    @Test
    public void testRegExQueryStatsOn() throws Exception {
        QueryImpl q = configQuery("(SOURCE =~ 'M.*')", auths);
        q.addParameter("stats", "true");
        EdgeQueryLogic logic = runLogic(q, auths);

        List<String> expected = new ArrayList<>();
        expected.add("mars%00;jupiter AdjacentPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mars%00;earth AdjacentPlanets/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mars STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");
        expected.add("mars%00;asteroid_belt AdjacentCelestialBodies/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mercury%00;sun AdjacentCelestialBodies/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mercury STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");
        expected.add("moon%00;earth AdjacentCelestialBodies/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mercury%00;venus AdjacentPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mars%00;ceres AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [B]");
        expected.add("mars%00;ceres AdjacentDwarfPlanets/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [B]");
        compareResults(logic, factory, expected);
    }

    @Test
    public void testRegExQueryStatsOff() throws Exception {
        QueryImpl q = configQuery("(SOURCE =~ 'M.*')", auths);
        q.addParameter("stats", "false");
        EdgeQueryLogic logic = runLogic(q, auths);

        List<String> expected = new ArrayList<>();
        expected.add("mars%00;jupiter AdjacentPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mars%00;earth AdjacentPlanets/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mars%00;asteroid_belt AdjacentCelestialBodies/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mercury%00;sun AdjacentCelestialBodies/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("moon%00;earth AdjacentCelestialBodies/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mercury%00;venus AdjacentPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mars%00;ceres AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [B]");
        expected.add("mars%00;ceres AdjacentDwarfPlanets/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [B]");
        compareResults(logic, factory, expected);
    }

    @Test
    public void testRelationStatsOn() throws Exception {
        QueryImpl q = configQuery("(SOURCE =~ 'M.*') && (SINK == 'VENUS') && (RELATION == 'FROM-TO' || RELATION == 'TO-FROM')", auths);
        q.addParameter("stats", "true");
        EdgeQueryLogic logic = runLogic(q, auths);

        List<String> expected = new ArrayList<>();
        expected.add("mercury%00;venus AdjacentPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mars STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");
        expected.add("mercury STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");
        compareResults(logic, factory, expected);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnknownFunction() throws Exception {

        QueryImpl q = configQuery("SOURCE == 'SUN' && (filter:includeregex(SINK, 'earth|mars'))", auths);

        EdgeQueryLogic logic = runLogic(q, auths);

        List<String> expected = new ArrayList<>();

        compareResults(logic, factory, expected);
    }

    @Test
    public void testComplexQuery2() throws Exception {

        QueryImpl q = configQuery(
                        "(SOURCE == 'EARTH' || SOURCE == 'SUN' || SOURCE == 'ASTEROID_BELT') &&" + " (SINK == 'MARS' || SINK == 'MOON' || SINK == 'JUPITER')",
                        auths);
        EdgeQueryLogic logic = runLogic(q, auths);

        List<String> expected = new ArrayList<>();
        expected.add("earth%00;moon AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("earth%00;mars AdjacentPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("asteroid_belt%00;mars AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("asteroid_belt%00;jupiter AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("earth STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");
        expected.add("sun STATS/ACTIVITY/Stars/TO:20150713/COSMOS_DATA [A]");

        compareResults(logic, factory, expected);
    }

    @Test
    public void testComplexQuery3() throws Exception {
        QueryImpl q = configQuery("(SOURCE == 'MARS' && SINK == 'JUPITER') || (SOURCE == 'ASTEROID_BELT' && SINK == 'MARS') "
                        + "|| (SOURCE == 'ASTEROID_BELT' && SINK == 'JUPITER')", auths);

        EdgeQueryLogic logic = runLogic(q, auths);

        List<String> expected = new ArrayList<>();
        expected.add("mars%00;jupiter AdjacentPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("asteroid_belt%00;mars AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("asteroid_belt%00;jupiter AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mars STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");
        compareResults(logic, factory, expected);
    }

    @Test
    public void testComplexQuery4() throws Exception {
        QueryImpl q = configQuery(
                        "(SOURCE == 'MARS' && SINK == 'JUPITER' && ATTRIBUTE1 == 'COSMOS_DATA-COSMOS_DATA' && ((TYPE == 'AdjacentPlanets' && RELATION == 'FROM-TO') || "
                                        + "(TYPE == 'AdjacentPlanets' && RELATION == 'TO-FROM' )))",
                        auths);

        EdgeQueryLogic logic = runLogic(q, auths);

        List<String> expected = new ArrayList<>();
        expected.add("mars%00;jupiter AdjacentPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        compareResults(logic, factory, expected);
    }

    @Test
    public void testAttribute1() throws Exception {
        QueryImpl q = configQuery("SOURCE == 'PLUTO' && ATTRIBUTE1 == 'NEW_HORIZONS-NEW_HORIZONS'", auths);

        EdgeQueryLogic logic = runLogic(q, auths);

        List<String> expected = new ArrayList<>();
        expected.add("pluto%00;neptune AdjacentDwarfPlanets/FROM-TO:20150713/NEW_HORIZONS-NEW_HORIZONS [C]");
        expected.add("pluto%00;neptune AdjacentPlanets/TO-FROM:20150713/NEW_HORIZONS-NEW_HORIZONS [C]");
        expected.add("pluto STATS/ACTIVITY/DwarfPlanets/TO:20150713/NEW_HORIZONS [D]");
        expected.add("pluto%00;charon AdjacentCelestialBodies/FROM-TO:20150713/NEW_HORIZONS-NEW_HORIZONS [C]");
        compareResults(logic, factory, expected);
    }

    public class TestQueryLogicFactory implements QueryLogicFactory {

        @Override
        public QueryLogic<?> getQueryLogic(String name, ProxiedUserDetails currentUser) throws QueryException {
            Set<String> userRoles = new HashSet<>(currentUser.getPrimaryUser().getRoles());
            return getQueryLogic(name, userRoles, true);
        }

        @Override
        public QueryLogic<?> getQueryLogic(String name) throws QueryException {
            return getQueryLogic(name, null, false);
        }

        private QueryLogic<?> getQueryLogic(String name, Collection<String> userRoles, boolean checkRoles) throws QueryException {
            QueryLogic<?> logic;
            try {
                logic = (QueryLogic<?>) applicationContext.getBean(name);
            } catch (ClassCastException | NoSuchBeanDefinitionException cce) {
                throw new IllegalArgumentException("Logic name '" + name + "' does not exist in the configuration");
            }

            if (checkRoles && !logic.canRunQuery(userRoles)) {
                throw new UnauthorizedQueryException(DatawaveErrorCode.MISSING_REQUIRED_ROLES,
                                new IllegalAccessException("User does not have required role(s): " + logic.getRequiredRoles()));
            }

            logic.setLogicName(name);
            return logic;
        }

        @Override
        public List<QueryLogic<?>> getQueryLogicList() {
            Map<String,QueryLogic> logicMap = applicationContext.getBeansOfType(QueryLogic.class);

            List<QueryLogic<?>> logicList = new ArrayList<>();

            for (Map.Entry<String,QueryLogic> entry : logicMap.entrySet()) {
                QueryLogic<?> logic = entry.getValue();
                logic.setLogicName(entry.getKey());
                logicList.add(logic);
            }
            return logicList;

        }
    }

}
