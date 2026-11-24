package example.marking.spi;

import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import org.apache.accumulo.core.security.ColumnVisibility;

/** A deployment's marking policy and codec. Neither operation may ignore an unsupported or invalid envelope. */
public interface MarkingProvider {
    /** Combine a nonempty collection, preserving provider metadata and all input access constraints. */
    MarkingEnvelope combine(Collection<MarkingEnvelope> inputs);

    /** Project the access constraints only; the result need not encode all of the marking. */
    ColumnVisibility toColumnVisibility(MarkingEnvelope input);

    /** Resolve once at application startup. Never choose the first of multiple providers or fall back to a public visibility. */
    static MarkingProvider load() {
        List<ServiceLoader.Provider<MarkingProvider>> candidates = ServiceLoader.load(MarkingProvider.class, MarkingProvider.class.getClassLoader()).stream()
                        .limit(2).collect(Collectors.toList());
        if (candidates.isEmpty()) {
            throw new IllegalStateException("No MarkingProvider registered");
        }
        if (candidates.size() > 1) {
            throw new IllegalStateException("Multiple MarkingProviders registered");
        }
        return candidates.get(0).get();
    }
}
