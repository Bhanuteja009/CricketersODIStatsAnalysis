import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducerTest extends Reducer<Text,IntWritable,Text,DoubleWritable> {

    @Override
    public void reduce(Text key,Iterable<IntWritable> values,Context context){
        int c=0;
        for(IntWritable value:values){
            c+=value.get();
        }

        context.write(key, c);
    }
    
}
