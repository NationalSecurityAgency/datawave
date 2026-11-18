package datawave.core.query.logic;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.marking.MarkingFunctions;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.webservice.query.result.event.ResponseObjectFactory;

/**
 * Builds a {@link ResponseEnricher} for a query. Response objects produced by query logics carry Accumulo column visibility strings (see
 * {@code HasColumnVisibility}); a {@link ResponseEnricher} is the hook where a deployment can translate those visibilities into its own security markings,
 * using the current user (and therefore the user's authorizations).
 */
public interface ResponseEnricherBuilder {
    public ResponseEnricherBuilder withConfig(GenericQueryConfiguration config);

    /**
     * @param functions
     *            ignored
     * @return this builder
     * @deprecated query logics no longer supply {@link MarkingFunctions}; enrichers should translate the column visibility of response objects themselves.
     */
    // TODO remove-accumulo-utils
    @Deprecated(forRemoval = true)
    public default ResponseEnricherBuilder withMarkingFunctions(MarkingFunctions functions) {
        return this;
    }

    public ResponseEnricherBuilder withResponseObjectFactory(ResponseObjectFactory factory);

    public ResponseEnricherBuilder withCurrentUser(ProxiedUserDetails user);

    public ResponseEnricherBuilder withServerUser(ProxiedUserDetails user);

    public ResponseEnricher build();
}
