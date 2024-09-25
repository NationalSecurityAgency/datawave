package datawave.core.common.cache;

import org.apache.curator.framework.listen.Listenable;

/**
 *
 *
 */
public interface SharedTriStateReader extends Listenable<SharedTriStateListener> {

    SharedTriState.STATE getState();

}
