package stockanalysis;

import java.io.IOException;
import java.io.IOException;
import java.util.Date;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.InternalCachedBag;

public class GrowthFactor extends EvalFunc<Float>{

    public Float exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0){
            return null;
        }
        // Convert the BinSeDesTuple to regular Tuple for processing.
        Tuple inputBag = (Tuple)input.get(0);
        float growth_factor;
        // Get the start and end "open" prices.
        float start = new Float(inputBag.get(0).toString()).floatValue();
        float end = new Float(inputBag.get(1).toString()).floatValue();
        // Compute the growth factor by last available open price / first available open price.
        growth_factor = end / start;
        // Return the growth factor.
        return new Float(growth_factor);
    }
}