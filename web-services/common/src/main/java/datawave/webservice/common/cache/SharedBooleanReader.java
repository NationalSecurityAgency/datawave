package datawave.webservice.common.cache;

import org.apache.curator.framework.listen.Listenable;

/**
 *
 *
 */
public interface SharedBooleanReader extends Listenable<SharedBooleanListener> {

    boolean getBoolean();

}
