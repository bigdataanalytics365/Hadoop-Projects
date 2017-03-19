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
        InternalCachedBag inputBag = (InternalCachedBag)input.get(0);
        for (Tuple data : inputBag) {
            float growth_factor;
            float start = new Float(data.get(1).toString()).floatValue();
            float end = new Float(data.get(2).toString()).floatValue();
            growth_factor = end / start;
            return new Float(growth_factor);
        }
        // Ideally it should never get here. For compiling purposes default to 0.
        return new Float(0);
    }
}