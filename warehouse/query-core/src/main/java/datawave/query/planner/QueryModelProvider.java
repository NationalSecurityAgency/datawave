package datawave.query.planner;

import datawave.query.model.QueryModel;

/**

 */
public interface QueryModelProvider {

    QueryModel getQueryModel();

    abstract class Factory {

        public abstract QueryModelProvider createQueryModelProvider();

    }
}
