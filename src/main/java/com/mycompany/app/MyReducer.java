package com.mycompany.app;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author ajay
 */
public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    
    @Override
    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
        //goal 1 : Bowling average and batting average above 40
    //     double bowlAvg = 0.0;
    //     double batAvg = 0.0;      
    //     for(List<DoubleWritable> value: values){
    //         bowlAvg=value.get(0).get();
    //         batAvg=value.get(1).get();
    //     }
    //     List<DoubleWritable> averages=new ArrayList<>();
    //     averages.add(new DoubleWritable(bowlAvg));
    //     averages.add(new DoubleWritable(batAvg));
    //    if(bowlAvg>40 && batAvg>40){
    //     context.write(key, averages);
    //    }    

    //goal 2: players who bowled more than 150 maiden overs
    // int maidens = 0;
    // for(IntWritable value:values){
    //     maidens=value.get();
    // }
    //     if(maidens>150){
    //         context.write(key, new IntWritable(maidens));
    //     }

    //goal 3: players who scored more than 20 centuries
    // int centuries = 0;
    // for(IntWritable value:values){
    //     maidens=value.get();
    // }
    //     if(centuries>20){
    //         context.write(key, new IntWritable(centuries));
    //     }

    //goal 4: players who took more than 250 wickets
    // int wickets = 0;
    // for(IntWritable value:values){
    //     wickets=value.get();
    // }
    //     if(wickets>250){
    //         context.write(key, new IntWritable(wickets));
    //     }

    //goal 5: players who took more than 145 catches
    // int catches = 0;
    // for(IntWritable value:values){
    //     catches=value.get();
    // }
    //     if(catches>145){
    //         context.write(key, new IntWritable(catches));
    //     }

    //goal 6: players who remained not out in  more than 50 matches
    int notOuts = 0;
    for(IntWritable value:values){
        notOuts=value.get();
    }
        if(notOuts>20){
            context.write(key, new IntWritable(notOuts));
        }
    
    }
}