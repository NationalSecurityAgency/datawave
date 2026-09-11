# Task 08 — PhraseHit and Backward-Compatible AllHitsFactory Extension

## Objective

Introduce mixed standalone/phrase hit handling and phrase-wide context without breaking existing configurable `AllHitsFactory` subclasses or response models.

## Prerequisite

Tasks 01–07 validation gates passed.

## Read first

- `src/main/java/datawave/query/transformer/annotation/AllHitsFactory.java` completely
- `src/main/java/datawave/query/transformer/annotation/AnnotationHitsTransformer.java` — `SegmentHit`
- `src/main/java/datawave/query/transformer/annotation/model/AllHit.java`
- `src/main/java/datawave/query/transformer/annotation/model/AllHits.java`
- `src/main/java/datawave/query/transformer/annotation/model/Term.java`
- `src/main/java/datawave/query/transformer/annotation/model/TermHit.java`
- `src/test/java/datawave/query/transformer/annotation/AllHitsFactoryTest.java`
- `src/test/java/datawave/query/transformer/annotation/AllHitsFactoryErrorOnly.java`

## Implement

1. Add a common internal `AnnotationHit` contract exposing context bounds, full hit span, and constituent `SegmentHit`s.
2. Make `SegmentHit` implement it while preserving constructors/equality used by existing tests.
3. Add `PhraseHit` containing:
   - constituent hits;
   - first/last hit boundary;
   - phrase-wide context start/end;
   - mode/distance if useful for equality/debugging.
4. Compute phrase context from earliest/latest phrase boundaries, extending `contextSize` sorted boundaries outside the phrase on each side.
5. Add a distinctly named factory method to avoid generic-erasure and source compatibility issues, e.g.:
   ```java
   createFromHits(String, List<? extends AnnotationHit>, TreeMap<...>, TimeUnit)
   ```
6. Preserve both existing `create(... List<SegmentHit> ...)` signatures.
7. `createFromHits` should expand phrase constituents with phrase-wide context and delegate through the existing virtual `create` method so subclasses overriding it still intercept calls.
8. Preserve response model and confidence/one-best behavior.
9. Collapse identical output hits once, consistent with the existing `TreeSet<TermHit>`, even if a value is both standalone and a phrase constituent.

Do not change `AllHits` JSON fields.

## Tests

- Existing factory tests unchanged.
- Mixed standalone and phrase hits.
- Phrase components all marked as hits.
- Context extends outside full phrase, not around each component independently.
- Context truncates at annotation ends.
- Overlapping contexts merge as current behavior dictates.
- Same value standalone + phrase constituent serializes once.
- Existing `AllHitsFactoryErrorOnly` override is invoked through `createFromHits`.
- JSON model remains unchanged.

## Validation gate

Before Task 09:

- All old and new factory tests pass.
- Custom subclass interception is proven by test.
- No response model class changed unless only non-serialized implementation detail was necessary.
- Ordered/unordered matcher tests pass with occurrence-to-`PhraseHit` adaptation.

Suggested command:

```bash
mvn -pl warehouse/query-core -am -DskipITs \
  -Dtest=AllHitsFactoryTest,AnnotationHitFactoryTest,OrderedAnnotationMatcherTest,UnorderedAnnotationMatcherTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```
