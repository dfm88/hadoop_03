package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
        NullWritable,           // Input key type
        WordCountWritable,    // Input value type
        Text,           // Output key type
        IntWritable> {  // Output value type

    @Override
    protected void reduce(
            NullWritable key, // Input key type
            Iterable<WordCountWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {
        TopKVector<WordCountWritable> top100 = new TopKVector<WordCountWritable>(100);

        /* Implement the reduce method */
        for (WordCountWritable wcw : values) {
            top100.updateWithNewElement(new WordCountWritable(wcw));
        }

        Vector<WordCountWritable> top100bjects = top100.getLocalTopK();
        for (WordCountWritable wcwWinner : top100bjects) {
            context.write(new Text(wcwWinner.getWord()), new IntWritable(wcwWinner.getCount()));

        }

    }
}
