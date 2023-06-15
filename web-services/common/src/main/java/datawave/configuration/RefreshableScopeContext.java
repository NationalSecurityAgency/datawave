package datawave.configuration;

import org.jboss.weld.contexts.AbstractSharedContext;

import java.lang.annotation.Annotation;

/**
 * The context for storing beans marked as {@link RefreshableScope}. These beans are all grouped together in a context that is invalidated upon receipt of a
 * {@link RefreshEvent}, thus causing all beans in the context to be destroyed and re-created.
 */
public class RefreshableScopeContext extends AbstractSharedContext {
    public RefreshableScopeContext() {
        super("STATIC_INSTANCE");
    }

    @Override
    public Class<? extends Annotation> getScope() {
        return RefreshableScope.class;
    }
}
