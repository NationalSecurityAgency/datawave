package datawave.webservice.query.logic;

import datawave.marking.MarkingFunctions;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.result.event.ResponseObjectFactory;

import java.security.Principal;

public interface ResponseEnricherBuilder {
    public ResponseEnricherBuilder withConfig(GenericQueryConfiguration config);

    public ResponseEnricherBuilder withMarkingFunctions(MarkingFunctions functions);

    public ResponseEnricherBuilder withResponseObjectFactory(ResponseObjectFactory factory);

    public ResponseEnricherBuilder withPrincipal(Principal principal);

    public ResponseEnricher build();
}
