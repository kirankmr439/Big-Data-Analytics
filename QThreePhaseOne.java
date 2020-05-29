import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class QThreePhaseOne
{
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
    {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
        
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
	    String doc = value.toString();
	    String docPart[] = doc.split(" "); //spliting input string to get individual words
	    String docName = docPart[0]; //getting the document number or the document name
	    String tempStr=""; //temp string to construct the key part
	    //loop to collect all the words
	    //for loop counter i is starting as we have first element of each line as document number
	    for(int i=1;i<docPart.length-1;i++)
	    {
		tempStr = (docPart[i]+" "+docPart[i+1]).replaceAll("\\p{P}", ""); //adding  the ith and i+1 word with space to form a bigram and   removing special character and punctuation from the word
		tempStr = tempStr+","+docName;
		word.set(tempStr);//converting string to text writable
		context.write(word,one);
	    }
	}
    } 
        
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
    {

	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	    throws IOException, InterruptedException
	    {
		int sum = 0;
		for (IntWritable val : values) 
		{
		    sum += val.get();
		}
		
		context.write(key, new IntWritable(sum));
		
	    }
    }
        
    public static void main(String[] args) throws Exception
    {
	Configuration conf = new Configuration();
        
        Job job = new Job(conf, "QThreePhaseOne");
    
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	job.setJarByClass(QThreePhaseOne.class);

	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
        
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
        
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
	job.waitForCompletion(true);
    }
        
}