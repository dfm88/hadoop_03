package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    LongWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */

        String[] splitted = value.toString().split(",");
        List<String> splittedList = Arrays.asList(splitted);
        // pop the first element that is the userID
        splittedList.remove(0);
        // order the item list to create unique pairs
        System.out.println("Before sort");
        System.out.println(splittedList.toString());
        Collections.sort(splittedList);
        System.out.println("After sort");
        System.out.println(splittedList.toString());
        // loop to the reviewed items if there are at least 2 of theme
        if(splittedList.size()>=2) {
            // loop length excluding th first that is the user id
            // starting loop from second element
            for(int i=0; i<splittedList.size(); i++){
                for(int j=0; j<splittedList.size(); j++){
                    if(j>i){
                        String itemPairs = splittedList.get(i)+','+splittedList.get(j);
                        context.write(new Text(itemPairs), new LongWritable(1));
                    }
                }
            }
        }

    }
}
