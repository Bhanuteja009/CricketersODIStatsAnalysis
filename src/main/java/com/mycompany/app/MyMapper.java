package com.mycompany.app;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 *
 * @author ajay
 * Modified by: Nathan Eloe
 * 
 */

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    
    @Override
    public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
        String[] row = value.toString().split("\t");  
        //goal 1 : Bowling average and batting average above 40
        // double bowlAvg=Double.parseDouble(row[2]);
        // double batAvg=Double.parseDouble(row[3]);
        //  List<DoubleWritable> averages=new ArrayList<>();
        //  averages.add(new DoubleWritable(bowlAvg));
        //  averages.add(new DoubleWritable(batAvg));
        //  context.write(new Text(row[0]), averages);

        //goal 2: players who bowled more than 150 maiden overs
        // int maidenOvers=Integer.parseInt(row[6]);
        // context.write(new Text(row[0]), new IntWritable(maidenOvers));

        //goal 3: players who scored more than 20 centuries
        // int centuries=Integer.parseInt(row[1]);
        // context.write(new Text(row[0]), new IntWritable(centuries));

        //goal 4: players who took more than 250 wickets
        // int wickets=Integer.parseInt(row[10]);
        // context.write(new Text(row[0]), new IntWritable(wickets));

        //goal 5: players who took more than 145 catches
        // int catches=Integer.parseInt(row[11]);
        // context.write(new Text(row[0]), new IntWritable(catches));

        //goal 6: players who remained not out in  more than 50 matches
        int notOuts=Integer.parseInt(row[7]);
        context.write(new Text(row[0]), new IntWritable(notOuts));
        
    }
    
}