package datawave.configuration;

/**
 * A CDI event class. This event is fired by {@link ConfigurationBean} when a configuration reload is requested. The {@link RefreshableScopeExtension} listens
 * for this event and causes all beans in the {@link RefreshableScope} to be re-created. Callers can also listen for this event and take appropriate behavior:
 *
 * <pre>
 * void onRefresh(@Observes RefreshEvent event, BeanManager bm) {
 *     // take appropriate action for a refresh here
 * }
 * </pre>
 */
public class RefreshEvent {}
