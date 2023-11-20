import org.apache.hadoop.fs.shell.Display.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapperTest extends Mapper<LongWritable,Text,Text,IntWritable> {


    @Override
    public void map(LongWritable key,Text value,Context context){
        String[] row=value.toString().split("\t");
        context.write(new Text(row[1]), new IntWritable(1));

    }
    
}
