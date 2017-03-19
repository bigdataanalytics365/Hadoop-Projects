package stockanalysis;

import java.io.IOException;
import java.io.IOException;
import java.util.Date;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;

public class StartDate extends EvalFunc<Long>{

    public Long exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0){
            return null;
        }
        // Convert the Tuple to DataBag for processing.
        DataBag inputBag = (DataBag)input.get(0);
        String date = "";
        int i = 1;
        for (Tuple temp : inputBag) {
            // Return on the 20th day. This could vary based on company data.
            if(i == 20){
                date = temp.toString().substring(1,temp.toString().length()-1);
                return new Long(Long.parseLong(date));
            }
            i++;
        }
        // Ideally it should never get here. This is only a place holder for compiling this file.
        return new Long(Long.parseLong(date));
    }
}