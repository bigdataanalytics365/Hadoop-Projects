package stockanalysis;

import java.io.IOException;
import java.util.*;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataBag;

public class MovingAverage extends EvalFunc<DataBag>{
    
    Public final BagFactory bagFactory = BagFactory.getInstance();
    Public final TupleFactory tupleFactory = TupleFactory.getInstance();

    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0){
            return null;
        }
        // Input data bag.
        DataBag inputBag = (DataBag)input.get(0);
        // Output DataBag. This bag contains the moving average for the entire month of october for this particular company.
        DataBag output = bagFactory.newDefaultBag();
        // Prices to take the average of.
        LinkedList<float> prices = new LinkedList<float>();
        // Counters for days in october and number of prices respectively.
        int day = 1;
        int i = 1;
        // Last moving average;
        float lastMovingAverage;
        
        for (Tuple temp : inputBag) {
            // Open price of stock for this day.
            float stockPrice = new Float(temp.get(2).toString()).floatValue();            
            // Check if we need to start computing moving average
            if(i > 20){
                // Average Record for this day for this company.
                Tuple averageRecord = tupleFactory.newTuple();
                // Append the company name to this tuple.
                averageRecord.append(temp.get(0).toString());
                // Append the date to this tuple.
                averageRecord.append(temp.get(1).toString());

                int stockDay = Integer.parseInt(temp.get(1).toString().substring(6,8));
                // Number of days this stock was not shown.
                int dayDiff = stockDay - day;
                for(int d=day; d<(dayDiff+day); d++){
                    // Append the averageRecord to output for missing days from last available prices.
                    Tuple t = tupleFactory.newTuple();
                    t.append(temp.get(0).toString());
                    if(day < 10){
                        t.append("2013100"+day.toString());
                    }else{
                        t.append("201310"+day.toString());
                    }
                    t.append(lastMovingAverage);
                }
                // Start computing averages.
                day = stockDay + 1;
            }else{
                // Buildup the price values to use.
                prices.add(openPrice);
            }
            i++;
        }
        // Output the DataBag we have constructed here. This bag contains the moving average for the entire month of october for this particular company.
        return output;
    }
    
    public float computeAverage(LinkedList<float> prices){
        float total;
        for (int i = 0; i < prices.size(); i++) {
			total += prices.get(i);
		}
        return total / prices.size();
    }
}