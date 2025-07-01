package datawave.query.jexl.visitors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.accumulo.inmemory.InMemoryAccumulo;
import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.configuration.QueryData;
import datawave.ingest.mapreduce.handler.dateindex.DateIndexUtil;
import datawave.microservice.query.QueryImpl;
import datawave.query.QueryParameters;
import datawave.query.composite.CompositeMetadataHelper;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveQueryException;
import datawave.query.exceptions.InvalidQueryTreeException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.util.JexlQueryGenerator;
import datawave.query.jexl.visitors.validate.ASTValidator;
import datawave.query.model.QueryModel;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.postprocessing.tf.PhraseIndexes;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.AllFieldMetadataHelper;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MockDateIndexHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.QueryModelVisitor;
import datawave.query.jexl.visitors.PrintingVisitor;
import datawave.query.util.MockMetadataHelper;
import datawave.query.util.TypeMetadataHelper;
import datawave.test.JexlNodeAssert;
import datawave.util.TableName;
import datawave.util.time.DateHelper;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class FindInvalidPreQueryModelTest {

    private static final int MAX_ATTEMPTS = 10_000;
    private static final Random RNG = new Random();
    private static final List<String> CANON_FIELDS = Arrays.asList("A", "B", "C", "D", "E", "F", "G");
    private static final List<String> CANON_VALUES = Arrays.asList("pickle", "banana", "carrot", "apple", "durian", "pineapple", "lard");

    private final SimpleDateFormat filterFormat = new SimpleDateFormat("yyyyMMdd:HH:mm:ss:SSSZ");
    private final ASTValidator validator = new ASTValidator();

    private DefaultQueryPlanner planner;
    private ShardQueryConfiguration config;
    private QueryImpl settings;
    private MockDateIndexHelper dateIndexHelper;
    private ASTJexlScript queryTree;
    private ScannerFactory scannerFactory;
    private AccumuloClient client;
    // utility class created to get around some exception finding composite terms
    class SethsMockMetadataHelper extends MockMetadataHelper {

        public SethsMockMetadataHelper(AllFieldMetadataHelper afmh) {
            super(afmh);
        }


        /**
         * Get reverse index fields using the data type filter.
         *
         * @param ingestTypeFilter
         *            the ingest type filter
         * @return the set of reverse indexed fields given the provided ingest type filter
         * @throws TableNotFoundException
         *             if the table does not exist
         */
        @Override
        public Set<String> getReverseIndexedFields(Set<String> ingestTypeFilter) throws TableNotFoundException {


            // Fields to datatypes for our MetadataHelper
            Multimap<String,String> fieldsToDatatypes = HashMultimap.create();
            fieldsToDatatypes.put("A", "datatype1");
            fieldsToDatatypes.put("B", "datatype2");
            fieldsToDatatypes.put("C", "datatype3");
            fieldsToDatatypes.put("D", "datatype4");
            addFieldsToDatatypes(fieldsToDatatypes);

            Multimap<String, String> indexedFields = this.allFieldMetadataHelper.loadReverseIndexedFields();

            Set<String> fields = new HashSet<>();
            if (ingestTypeFilter == null || ingestTypeFilter.isEmpty()) {
                fields.addAll(indexedFields.values());
            } else {
                for (String datatype : ingestTypeFilter) {
                    fields.addAll(indexedFields.get(datatype));
                }
            }
            return Collections.unmodifiableSet(fields);
        }

        @Override
        public Map<String,Date> getWhindexCreationDateMap(Set<String> ingestTypeFilter) throws TableNotFoundException {
            return new HashMap<>();
        }
    }
    @BeforeEach
    public void setUp() throws AccumuloSecurityException {

        client = new InMemoryAccumuloClient("seth", new InMemoryInstance("seth's cool client"));

        final Set<Authorizations> allMetadataAuths = Collections.emptySet();
        final HashSet<Authorizations> auths = new HashSet<>();
        auths.add(new Authorizations("ALL"));
        TypeMetadataHelper tmh = new TypeMetadataHelper(Maps.newHashMap(), allMetadataAuths, client, TableName.METADATA, auths, false);
        CompositeMetadataHelper cmh = new CompositeMetadataHelper(client, TableName.METADATA, auths);
        AllFieldMetadataHelper afmh = new AllFieldMetadataHelper(tmh, cmh, client, TableName.METADATA, auths, allMetadataAuths);

        SethsMockMetadataHelper mockMetadataHelper = new SethsMockMetadataHelper(afmh);

        planner = new DefaultQueryPlanner();

        planner.setMetadataHelper(mockMetadataHelper);

        planner.setDateIndexHelper(new MockDateIndexHelper());


        config = new ShardQueryConfiguration();
        config.setClient(client);

        HashSet<Authorizations> authorizationsHashSet = new HashSet<>();
        authorizationsHashSet.add(new Authorizations("ALL"));

        config.setAuthorizations(authorizationsHashSet);
        settings = new QueryImpl();
        dateIndexHelper = new MockDateIndexHelper();

        scannerFactory = new ScannerFactory(config);

    }

    @Test
    public void testBasicDateFilter() throws DatawaveQueryException, ParseException {
        queryTree = JexlASTHelper.parseJexlQuery("FOO == 'bar'");
        config.setDefaultDateTypeName("EVENT");
        config.setNoExpansionIfCurrentDateTypes(Set.of("OTHER_EVENT"));
        Date beginDate = DateHelper.parse("20241001");
        Date endDate = new Date();
        config.setBeginDate(beginDate);
        config.setEndDate(endDate);

        settings.addParameter(QueryParameters.DATE_RANGE_TYPE, "SPECIAL_EVENT");
        dateIndexHelper.addEntry("20241010", "SPECIAL_EVENT", "wiki", "FOO", "20241010_shard");

        // Execute the planner
        Iterable<QueryData> plans = planner.process(
                config,
                JexlStringBuildingVisitor.buildQuery(queryTree),
                settings,
                scannerFactory
        );
        // Ensure at least one plan was generated
        Assertions.assertFalse(plans.iterator().hasNext(), "Planner generated no query plans");
    }

    @Test
    public void findFirstInvalidConfig() throws Exception {
        // Prepare a base query for generation
        queryTree = JexlASTHelper.parseJexlQuery("FOO == 'bar'");

        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            System.out.printf("%n--- Attempt %d of %d ---%n", attempt, MAX_ATTEMPTS);

            /* ───────────────────────── 1) VALIDATOR FLAGS ───────────────────────── */
            validator.enableAll();          // or flip random bits if you wish
            dumpValidatorFlags();

            /* ───────────────────────── 2) RANDOM FIELD / VALUE SETS ───────────────────────── */
            Set<String> fields  = randomSubset(CANON_FIELDS);
            Set<String> values  = randomSubset(CANON_VALUES);
            if (fields.isEmpty())  fields.add("A");
            if (values.isEmpty())  values.add("pickle");
            System.out.printf("Fields: %s, Values: %s%n", fields, values);

            JexlQueryGenerator gen = new JexlQueryGenerator(fields, values);
            randomizeGeneratorOptions(gen);
            String original = gen.getQuery(100);
            System.out.println("Original generated query: " + original);



            /* ───────────────────────── 3) RANDOM QUERY-MODEL ───────────────────────── */
            QueryModel qm      = randomQueryModel(fields);
            Set<String> allFld = new HashSet<>(fields);
            allFld.addAll(qm.getForwardQueryMapping().keySet());
            allFld.addAll(qm.getForwardQueryMapping().values());

            /* ───────────────────────── 4) PLAN & VALIDATE *EACH* QUERY ───────────────────────── */
            Iterable<QueryData> plans = planner.process(
                    config,
                    JexlStringBuildingVisitor.buildQuery(JexlASTHelper.parseJexlQuery(original)),
                    settings,
                    scannerFactory
            );

            Iterator<QueryData> planIter = plans.iterator();
            if (!planIter.hasNext()) {
                // nothing came back – skip this run and try again
                continue;
            }

            while (planIter.hasNext()) {
                QueryData qd      = planIter.next();
                String planQuery  = qd.getQuery();
                ASTJexlScript pre = JexlASTHelper.parseJexlQuery(planQuery);

                // apply model, then validate
                ASTJexlScript postModel = QueryModelVisitor.applyModel(pre, qm, allFld);
                boolean ok = validator.isValid(postModel);
                System.out.printf("  └─ plan \"%s\" ⇒ %s%n",
                        planQuery.replace('\n', ' ').trim(),
                        ok ? "VALID" : "INVALID");

                if (!ok) {
                    // Found a bad one – report & fail
                    System.out.println("\n══════════════════════════════════════════════════════════════════════");
                    System.out.printf ("Found invalid configuration on attempt %d%n", attempt);
                    System.out.println("–––––  Original Query  ––––––––––––––––––––––––––––––––––––––––––––––");
                    System.out.println(original);
                    System.out.println("–––––  Failing Plan Query  ––––––––––––––––––––––––––––––––––––––––––");
                    System.out.println(planQuery);
                    System.out.println("–––––  Post-Model JEXL  –––––––––––––––––––––––––––––––––––––––––––––");
                    PrintingVisitor.printQuery(postModel);
                    System.out.println("–––––  Query-Model Mapping  –––––––––––––––––––––––––––––––––––––––––");
                    qm.dumpAttributes(System.out);
                    System.out.println("══════════════════════════════════════════════════════════════════════\n");
                    Assertions.fail("Post-QM query failed validation – see console for details.");
                }
            }
            // if we got here, *all* plans for this attempt were valid – next attempt
        }
        System.out.printf("No invalid configuration found after %,d attempts.%n", MAX_ATTEMPTS);
    }


// Helper methods

    private static <T> Set<T> randomSubset(List<T> source) {
        Set<T> out = new HashSet<>();
        for (T item : source) {
            if (RNG.nextBoolean()) {
                out.add(item);
            }
        }
        return out;
    }

    private static void randomizeGeneratorOptions(JexlQueryGenerator gen) {
        if (RNG.nextBoolean()) gen.enableAllOptions();
        else gen.disableAllOptions();
    }

    private static QueryModel randomQueryModel(Set<String> baseFields) {
        QueryModel qm = new QueryModel();
        for (String f : baseFields) {
            int clones = 1 + RNG.nextInt(3);
            for (int i = 0; i < clones; i++) {
                String alias = f + "_" + i;
                qm.addTermToModel(f, alias);
                qm.addTermToReverseModel(alias, f);
            }
        }
        return qm;
    }

    private void dumpValidatorFlags() {
        System.out.printf("  validateFlatten              = %s%n", validator.getValidateFlatten());
        System.out.printf("  validateJunctions            = %s%n", validator.getValidateJunctions());
        System.out.printf("  validateLineage              = %s%n", validator.isValidateLineage());
        System.out.printf("  validateReferenceExpressions = %s%n", validator.getValidateReferenceExpressions());
        System.out.printf("  validateQueryPropertyMarkers = %s%n", validator.getValidateQueryPropertyMarkers());
    }

}
