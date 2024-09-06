/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package datawave.core.common.cache;

import org.apache.curator.framework.listen.Listenable;

/**
 *
 *
 */
public interface SharedBooleanReader extends Listenable<SharedBooleanListener> {

    boolean getBoolean();

}
