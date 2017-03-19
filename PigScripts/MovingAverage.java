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
    
    public final BagFactory bagFactory = BagFactory.getInstance();
    public final TupleFactory tupleFactory = TupleFactory.getInstance();

    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0){
            return null;
        }
        // Input data bag.
        DataBag inputBag = (DataBag)input.get(0);
        // Output DataBag. This bag contains the moving average for the entire month of october for this particular company.
        DataBag output = bagFactory.newDefaultBag();
        // Prices to take the average of.
        LinkedList<Float> prices = new LinkedList<Float>();
        // Counters for days in october and number of prices respectively.
        int i = 1;
        int day = 1;
        // Last moving average;
        float lastMovingAverage = 0;
        
        for (Tuple temp : inputBag) {
            String companyName = temp.get(0).toString();
            // Open price of stock for this day.
            float stockPrice = new Float(temp.get(2).toString()).floatValue();            
            // Check if we need to start computing moving average
            if(i >= 20){
                int stockDay = Integer.parseInt(temp.get(1).toString().substring(6,8));
                // Number of days this stock was not shown.
                int dayDiff = stockDay - day;
                for(int d=day; d<(dayDiff+day); d++){
                    // Append the averageRecord to output for missing days from last available prices.
                    Tuple missingRecord = tupleFactory.newTuple();
                    // Append company name.
                    missingRecord.append(companyName);
                    // Append the date for this missingRecord.
                    if(day < 10){
                        missingRecord.append("2013100"+Integer.toString(d));
                    }else{
                        missingRecord.append("201310"+Integer.toString(d));
                    }
                    // Append open price and moving average.
                    missingRecord.append(stockPrice);
                    missingRecord.append(lastMovingAverage);
                    // Add this record to the output.
                    output.add(missingRecord);
                }
                // Start computing averages.
                day = stockDay + 1;
                // Compute the average for this day.
                float avg = computeAverage(prices);
                // Average Record for this day for this company.
                Tuple averageRecord = tupleFactory.newTuple();
                // Append the company name, date and average to this tuple.
                averageRecord.append(companyName);
                averageRecord.append(temp.get(1).toString());
                averageRecord.append(stockPrice);
                averageRecord.append(avg);
                // Add this tuple to the output bag.
                output.add(averageRecord);
                // Update the prices queue.
                prices.removeFirst();
                prices.add(new Float(stockPrice));
                lastMovingAverage = avg;
            }else{
                // Buildup the price values to use for first average.
                prices.add(new Float(stockPrice));
            }
            i++;
        }
        // Output the DataBag we have constructed here. This bag contains moving averages for the entire month of october for this particular company.
        return output;
    }
    
    public float computeAverage(LinkedList<Float> prices){
        float total = 0;
        for (int i = 0; i < prices.size(); i++) {
			total += prices.get(i).floatValue();
		}
        return total / prices.size();
    }
}