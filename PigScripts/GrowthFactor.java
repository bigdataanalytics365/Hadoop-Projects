package stockanalysis;

import java.io.IOException;
import java.io.IOException;
import java.util.Date;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;

public class GrowthFactor extends EvalFunc<Integer>{

    public Integer exec(Tuple input) throws IOException {
        // if (input == null || input.size() == 0 || input.size() != 2){
        if (input == null || input.size() == 0){
            return null;
        }
        // float growth_factor;
        // float start = (float) input.get(0).toString();
        // float end = (float) input.get(2);
        // growth_factor = end / start;
        return new Integer(input.size());
    }
}