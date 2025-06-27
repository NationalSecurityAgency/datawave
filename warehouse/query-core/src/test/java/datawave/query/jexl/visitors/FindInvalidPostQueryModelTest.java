package datawave.query.jexl.visitors;/*
 * Re‑worked fuzz‑style test that keeps mutating the validation, generator
 * and query‑model configuration until it produces a post‑query‑model JEXL
 * tree that **fails** validation.  When such a configuration is found the
 * test prints the details and exits – allowing you to inspect the exact
 * recipe that broke things.
 *
 * ‑ The outer loop mutates three things each attempt:
 *   1. Validator flag mix
 *   2. Field/value sets + JexlQueryGenerator options
 *   3. Query‑model mappings
 *
 * ‑ A sane MAX_ATTEMPTS guard is included so the test can finish even if no
 *   invalid configuration emerges (tweak or remove if you truly want an
 *   endless search).
 *
 * NOTE:  This assumes all helper classes (Validator, JexlQueryGenerator,
 * QueryModel, etc.) are available on the class‑path exactly as in your
 * original test.
 */

import static org.junit.Assert.*;

import datawave.query.exceptions.InvalidQueryTreeException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.util.JexlQueryGenerator;
import datawave.query.jexl.visitors.validate.ASTValidator;
import datawave.query.model.QueryModel;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.junit.Test;

import java.text.ParseException;
import java.util.*;

public class FindInvalidPostQueryModelTest {
    private static final Random RNG = new Random();
    private static final int MAX_ATTEMPTS = 10_000;   // set to Integer.MAX_VALUE for endless

    // --- shared fixtures (reuse between attempts) -----------------------------------------
    private final ASTValidator validator = new ASTValidator();

    private static final List<String> CANON_FIELDS   = Arrays.asList("A","B","C","D");
    private static final List<String> CANON_VALUES   = Arrays.asList("pickle","banana","carrot","apple","durian");

    @Test
    public void findFirstInvalidConfig() throws ParseException, InvalidQueryTreeException, org.apache.commons.jexl3.parser.ParseException {
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            System.out.printf("\n--- Attempt %d of %d ---\n", attempt, MAX_ATTEMPTS);

            // 1)  ─── MUTATE VALIDATOR FLAGS ────────────────────────────────────────────────
            validator.setValidateFlatten(             RNG.nextBoolean());
            validator.setValidateJunctions(           RNG.nextBoolean());
            validator.setValidateLineage(             RNG.nextBoolean());
            validator.setValidateReferenceExpressions(RNG.nextBoolean());
            validator.setValidateQueryPropertyMarkers(RNG.nextBoolean());
            dumpValidatorFlags();

            // 2)  ─── BUILD RANDOM FIELD + VALUE SETS  ──────────────────────────────────────
            Set<String> values = randomSubset(CANON_VALUES);
            Set<String> fields = randomSubset(CANON_FIELDS);
            // always keep at least one field/value around
            if (fields.isEmpty()) fields.add("A");
            if (values.isEmpty()) values.add("pickle");
            System.out.printf("Fields: %s, Values: %s\n", fields, values);

            JexlQueryGenerator gen = new JexlQueryGenerator(fields, values);
            randomizeGeneratorOptions(gen);
            String original = gen.getQuery(100);
            System.out.println("Original generated query: " + original);

            // 3)  ─── RANDOM QUERY‑MODEL MAPPINGS ───────────────────────────────────────────
            QueryModel qm = randomQueryModel(fields);
            Set<String> allFields = new HashSet<>(fields);
            allFields.addAll(qm.getForwardQueryMapping().keySet());
            allFields.addAll(qm.getForwardQueryMapping().values());

            // 4)  ─── APPLY MODEL + VALIDATE ────────────────────────────────────────────────
            ASTJexlScript groomed            = InvertNodeVisitor.invertSwappedNodes(JexlASTHelper.parseJexlQuery(original));
            ASTJexlScript postModel          = QueryModelVisitor.applyModel(groomed, qm, allFields);
            boolean       isValidAfterModel = validator.isValid(postModel);
            System.out.printf("Post-model validation: %s\n", (isValidAfterModel ? "VALID" : "INVALID"));

            if (!isValidAfterModel) {
                // ─── FOUND FAILURE CASE – REPORT + EXIT ──────────────────────────────────
                System.out.println("\n════════════════════════════════════════════════════════════════════");
                System.out.printf ("Found invalid configuration on attempt %d\n", attempt);
                System.out.println("–––––  Original Query  ––––––––––––––––––––––––––––––––––––––––––––––");
                System.out.println(original);
                System.out.println("–––––  Post‑Model JEXL  –––––––––––––––––––––––––––––––––––––––––––––");
                PrintingVisitor.printQuery(postModel);
                System.out.println("–––––  Query‑Model Mapping  –––––––––––––––––––––––––––––––––––––––––");
                qm.dumpAttributes(System.out);   // assumes you add a helper or use toString()
                System.out.println("════════════════════════════════════════════════════════════════════\n");
                // Fail the test so CI flags it – comment out if you *don't* want failure
                fail("Post‑QM query failed validation – see console for reproduction details.");
                return; // defensive – won't be hit after fail()
            }
        }
        // Nothing hit the failure condition within the attempt budget.
        System.out.printf("No invalid configuration found after %,d attempts.%n", MAX_ATTEMPTS);
    }

    /* --------------------------------------------------------------------- */
    /*  Helper Methods                                                       */
    /* --------------------------------------------------------------------- */

    private static <T> Set<T> randomSubset(List<T> source) {
        Set<T> out = new HashSet<>();
        for (T item : source) if (RNG.nextBoolean()) out.add(item);
        return out;
    }

    private static void randomizeGeneratorOptions(JexlQueryGenerator g) {
        // Turn all flags on/off randomly – extend as needed
        if (RNG.nextBoolean()) g.enableAllOptions();
        else                   g.disableAllOptions();
    }

    private static QueryModel randomQueryModel(Set<String> baseFields) {
        QueryModel qm = new QueryModel();
        // create between 1‑3 random forward mappings per field
        for (String f : baseFields) {
            int clones = 1 + RNG.nextInt(3);
            for (int i = 0; i < clones; i++) {
                String alias = f + "_" + i; // simple deterministic alias
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
