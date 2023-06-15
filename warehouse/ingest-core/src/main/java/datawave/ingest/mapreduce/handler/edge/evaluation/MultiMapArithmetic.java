package datawave.ingest.mapreduce.handler.edge.evaluation;

import org.apache.commons.jexl2.JexlArithmetic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class MultiMapArithmetic extends JexlArithmetic {

    Logger log = LoggerFactory.getLogger(MultiMapArithmetic.class);

    public MultiMapArithmetic(boolean lenient) {
        super(lenient);
    }

    @Override
    protected int compare(Object left, Object right, String operator) {

        log.trace("Comparing");

        if (left instanceof Collection && (right instanceof String || right instanceof Integer)) {

            /*
             * System.out.println("R: " + right.toString()); StringBuilder sb = new StringBuilder(); for( Object strObj : ((Set) left).toArray() ){
             * sb.append(strObj.toString()).append("."); } System.out.println("L: " + sb.toString());
             */

            if (((Collection) left).contains(right)) {

                return 0;

            }
        }

        return super.compare(left, right, operator);
    }
}
