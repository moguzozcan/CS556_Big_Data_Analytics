package tr.edu.ozu.cs556;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;


public class WordCountInitialLetter extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(WordCountInitialLetter.class);
	
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = new Configuration();
		String letter = arg0[2];
		conf.set("letter", letter);
		Job job = Job.getInstance(conf, "wordcountinitialletter");
	    job.setJarByClass(this.getClass());
	    FileInputFormat.addInputPath(job, new Path(arg0[0]));
	    FileOutputFormat.setOutputPath(job, new Path(arg0[1]));	    
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    return job.waitForCompletion(true) ? 0 : 1;
	}

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new WordCountInitialLetter(), args);
        System.exit(res);
    }

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {	
		private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();   
	
	    public void map(LongWritable offset, Text lineText, Context context)
	        throws IOException, InterruptedException {
	    	Configuration conf = context.getConfiguration();
			String letter = conf.get("letter");
			
	    	String line = lineText.toString();
	    	StringTokenizer itr = new StringTokenizer(line);
	        while (itr.hasMoreTokens()) {
	            String token = itr.nextToken();
	            if(token.startsWith(letter)){
	                word.set("Count of words start with letter: " + letter + " is: ");
	                context.write(word, one);
	            }
	        }
	    }
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	    @Override
	    public void reduce(Text word, Iterable<IntWritable> counts, Context context)
	        throws IOException, InterruptedException {
	    	int sum = 0;
	    	for (IntWritable count : counts) {
	    		sum += count.get();
	    	}
	    	context.write(word, new IntWritable(sum));
	    }
	}
}