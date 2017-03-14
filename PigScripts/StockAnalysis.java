import java.io.IOException;
import java.io.IOException;
import java.util.Date;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class StockAnalysis extends EvalFunc<String>{ 

    public String exec(Tuple input) throws IOException {   
        if (input == null || input.size() == 0){
            return null;      
        }
        String str = (String)input.get(0);
        return str.toUpperCase();
    }
}