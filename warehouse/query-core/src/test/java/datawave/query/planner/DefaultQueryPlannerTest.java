package datawave.query.planner;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import datawave.ingest.mapreduce.handler.dateindex.DateIndexUtil;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.QueryParameters;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MockDateIndexHelper;
import datawave.test.JexlNodeAssert;
import datawave.util.time.DateHelper;

class DefaultQueryPlannerTest {

    /**
     * Contains tests for
     * {@link DefaultQueryPlanner#addDateFilters(ASTJexlScript, ScannerFactory, MetadataHelper, DateIndexHelper, ShardQueryConfiguration, Query)}
     */
    @Nested
    class DateFilterTests {

        private final SimpleDateFormat filterFormat = new SimpleDateFormat("yyyyMMdd:HH:mm:ss:SSSZ");

        private DefaultQueryPlanner planner;
        private ShardQueryConfiguration config;
        private QueryImpl settings;
        private MockDateIndexHelper dateIndexHelper;
        private ASTJexlScript queryTree;

        @BeforeEach
        void setUp() {
            planner = new DefaultQueryPlanner();
            config = new ShardQueryConfiguration();
            settings = new QueryImpl();
            dateIndexHelper = new MockDateIndexHelper();
        }

        /**
         * Verify that when the date type is the default date type, and is part of the noExpansionIfCurrentDateTypes types, and the query's end date is the
         * current date, that no date filters are added and SHARDS_AND_DAYS hints are forbidden.
         */
        @Test
        void testDefaultDateTypeMarkedForNoExpansionAndEndDateIsCurrDate() throws Exception {
            queryTree = JexlASTHelper.parseJexlQuery("FOO == 'bar'");
            config.setDefaultDateTypeName("EVENT");
            config.setNoExpansionIfCurrentDateTypes(Set.of("EVENT"));

            Date beginDate = DateHelper.parse("20241001");
            config.setBeginDate(beginDate);
            Date endDate = new Date();
            config.setEndDate(endDate);

            ASTJexlScript actual = addDateFilters();

            JexlNodeAssert.assertThat(actual).isEqualTo("FOO == 'bar'");
            Assertions.assertFalse(config.isShardsAndDaysHintAllowed());
            Assertions.assertEquals(beginDate, config.getBeginDate());
            Assertions.assertEquals(endDate, config.getEndDate());
        }

        /**
         * Verify that when a date type is given via parameters that is part of the noExpansionIfCurrentDateTypes types, and the query's end date is the current
         * date, that no date filters are added and SHARDS_AND_DAYS hints are forbidden.
         */
        @Test
        void testParamDateTypeMarkedForNoExpansionAndEndDateIsCurrDate() throws Exception {
            queryTree = JexlASTHelper.parseJexlQuery("FOO == 'bar'");
            config.setDefaultDateTypeName("EVENT");
            config.setNoExpansionIfCurrentDateTypes(Set.of("SPECIAL_EVENT"));

            Date beginDate = DateHelper.parse("20241001");
            config.setBeginDate(beginDate);
            Date endDate = new Date();
            config.setEndDate(endDate);

            settings.addParameter(QueryParameters.DATE_RANGE_TYPE, "SPECIAL_EVENT");

            ASTJexlScript actual = addDateFilters();

            JexlNodeAssert.assertThat(actual).isEqualTo("FOO == 'bar'");
            Assertions.assertFalse(config.isShardsAndDaysHintAllowed());
            Assertions.assertEquals(beginDate, config.getBeginDate());
            Assertions.assertEquals(endDate, config.getEndDate());
        }

        /**
         * Verify that when the date type is the default date type, and is part of the noExpansionIfCurrentDateTypes types, but the query's end date is not the
         * current date, that no date filters are added and SHARDS_AND_DAYS hints are allowed.
         */
        @Test
        void testDefaultDateTypeMarkedForNoExpansionAndEndDateIsNotCurrDate() throws Exception {
            queryTree = JexlASTHelper.parseJexlQuery("FOO == 'bar'");
            config.setDefaultDateTypeName("EVENT");
            config.setNoExpansionIfCurrentDateTypes(Set.of("EVENT"));

            Date beginDate = DateHelper.parse("20241001");
            config.setBeginDate(beginDate);
            Date endDate = DateHelper.parse("20241010");
            config.setEndDate(endDate);

            ASTJexlScript actual = addDateFilters();

            JexlNodeAssert.assertThat(actual).isEqualTo("FOO == 'bar'");
            Assertions.assertTrue(config.isShardsAndDaysHintAllowed());
            Assertions.assertEquals(beginDate, config.getBeginDate());
            Assertions.assertEquals(endDate, config.getEndDate());
        }

        /**
         * Verify that when a date type is given via parameters that is part of the noExpansionIfCurrentDateTypes types, but the query's end date is not the
         * current date, that date filters are added and SHARDS_AND_DAYS hints are allowed.
         */
        @Test
        void testParamDateTypeMarkedForNoExpansionAndEndDateIsNotCurrDate() throws Exception {
            queryTree = JexlASTHelper.parseJexlQuery("FOO == 'bar'");
            config.setDefaultDateTypeName("EVENT");
            config.setNoExpansionIfCurrentDateTypes(Set.of("SPECIAL_EVENT"));
            Date beginDate = DateHelper.parse("20241009");
            config.setBeginDate(beginDate);
            Date endDate = DateHelper.parse("20241011");
            config.setEndDate(endDate);
            settings.addParameter(QueryParameters.DATE_RANGE_TYPE, "SPECIAL_EVENT");
            dateIndexHelper.addEntry("20241010", "SPECIAL_EVENT", "wiki", "FOO", "20241010_shard");

            ASTJexlScript actual = addDateFilters();

            JexlNodeAssert.assertThat(actual).hasExactQueryString(
                            "(FOO == 'bar') && filter:betweenDates(FOO, '" + filterFormat.format(beginDate) + "', '" + filterFormat.format(endDate) + "')");
            Assertions.assertTrue(config.isShardsAndDaysHintAllowed());
            Assertions.assertEquals(DateIndexUtil.getBeginDate("20241010"), config.getBeginDate());
            Assertions.assertEquals(DateIndexUtil.getEndDate("20241010"), config.getEndDate());
        }

        /**
         * Verify that when the date type is the default date type, and is not part of the noExpansionIfCurrentDateTypes types, and the query's end date is the
         * current date, that no date filters are added and SHARDS_AND_DAYS hints are allowed.
         */
        @Test
        void testDefaultDateTypeIsNotMarkedForNoExpansionAndEndDateNotCurrDate() throws Exception {
            queryTree = JexlASTHelper.parseJexlQuery("FOO == 'bar'");
            config.setDefaultDateTypeName("EVENT");
            config.setNoExpansionIfCurrentDateTypes(Set.of("OTHER_EVENT"));

            Date beginDate = DateHelper.parse("20241001");
            config.setBeginDate(beginDate);
            Date endDate = DateHelper.parse("20241010");
            config.setEndDate(endDate);

            ASTJexlScript actual = addDateFilters();

            JexlNodeAssert.assertThat(actual).isEqualTo("FOO == 'bar'");
            Assertions.assertTrue(config.isShardsAndDaysHintAllowed());
            Assertions.assertEquals(beginDate, config.getBeginDate());
            Assertions.assertEquals(endDate, config.getEndDate());
        }

        /**
         * Verify that when a date type is given via parameters that is not part of the noExpansionIfCurrentDateTypes types, and the query's end date is the
         * current date, that date filters are added and SHARDS_AND_DAYS hints are allowed.
         */
        @Test
        void testParamDateTypeIsNotMarkedForNoExpansionAndEndDateIsCurrDate() throws Exception {
            queryTree = JexlASTHelper.parseJexlQuery("FOO == 'bar'");
            config.setDefaultDateTypeName("EVENT");
            config.setNoExpansionIfCurrentDateTypes(Set.of("OTHER_EVENT"));
            config.setBeginDate(DateHelper.parse("20241009"));
            Date beginDate = DateHelper.parse("20241001");
            config.setBeginDate(beginDate);
            Date endDate = new Date();
            config.setEndDate(endDate);

            settings.addParameter(QueryParameters.DATE_RANGE_TYPE, "SPECIAL_EVENT");
            dateIndexHelper.addEntry("20241010", "SPECIAL_EVENT", "wiki", "FOO", "20241010_shard");

            ASTJexlScript actual = addDateFilters();

            JexlNodeAssert.assertThat(actual).hasExactQueryString(
                            "(FOO == 'bar') && filter:betweenDates(FOO, '" + filterFormat.format(beginDate) + "', '" + filterFormat.format(endDate) + "')");
            Assertions.assertTrue(config.isShardsAndDaysHintAllowed());
            Assertions.assertEquals(DateIndexUtil.getBeginDate("20241010"), config.getBeginDate());
            Assertions.assertEquals(DateIndexUtil.getEndDate("20241010"), config.getEndDate());
        }

        private ASTJexlScript addDateFilters() throws TableNotFoundException, DatawaveQueryException {
            return planner.addDateFilters(queryTree, null, null, dateIndexHelper, config, settings);
        }
    }
}
