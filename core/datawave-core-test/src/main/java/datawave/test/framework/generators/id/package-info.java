/**
 * This package provides various strategies for generating a distribution of event ids for a given bound, which is what determines how frequently a field
 * appears across the generated events.
 * <p>
 * Supported strategies:
 * <ul>
 * <li>{@link datawave.test.framework.generators.id.SequentialEventIdGenerator} - every id, {@code 1, 2, 3, ...}</li>
 * <li>{@link datawave.test.framework.generators.id.ModuloEventIdGenerator} - every nth id, {@code n, 2n, 3n, ...}, or another residue class when an offset is
 * set</li>
 * <li>{@link datawave.test.framework.generators.id.FibonacciEventIdGenerator} - {@code 1, 2, 3, 5, 8, ...}</li>
 * <li>{@link datawave.test.framework.generators.id.SquaresEventIdGenerator} - {@code 1, 4, 9, 16, ...}</li>
 * </ul>
 * {@code IngestMetadata} builds its automatic field space from the first two only. Fibonacci and Squares are supported options that no built-in configuration
 * uses yet; a test that assembles its own {@code FieldMetadata} may use either. See the module README.
 */
package datawave.test.framework.generators.id;
