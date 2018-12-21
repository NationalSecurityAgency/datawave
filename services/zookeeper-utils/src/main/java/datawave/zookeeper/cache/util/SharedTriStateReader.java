package datawave.zookeeper.cache.util;

import org.apache.curator.framework.listen.Listenable;

/**
 *
 *
 */
public interface SharedTriStateReader extends Listenable<SharedTriStateListener> {
    
    SharedTriState.STATE getState();
    
}
