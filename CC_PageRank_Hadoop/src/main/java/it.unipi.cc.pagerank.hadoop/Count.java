package it.unipi.cc.pagerank.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class Count {
    private static final String OUTPUT_PATH = "/count";
    private static final String OUTPUT_SEPARATOR = "-";
    private static final String OUTPUT_KEY = "Total Pages";

    private static Count instance = null;  // Singleton

    private Count() { }

    public static Count getInstance()
    {
        if (instance == null)
            instance = new Count();

        return instance;
    }

    public static class CountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable one = new IntWritable(1);
        private static final Text keyEmit = new Text(OUTPUT_KEY); // useful for debugging

        // For each line in the input, emit 1
        @Override
        public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
            context.write(keyEmit, one);
        }
    }

    public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        @Override
        public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (final IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public int getPageCount(final String input, final String baseOutput) throws Exception {
        // run the Count stage
        if(!Count.getInstance().run(input, baseOutput))
            return -1;

        // read and return the result
        final String file = baseOutput + OUTPUT_PATH + "/part-r-00000";

        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = reader.readLine();
        reader.close();

        String[] tokens = line.trim().split(OUTPUT_SEPARATOR);
        assertEquals(OUTPUT_KEY, tokens[0]);

        return Integer.parseInt(tokens[1]);
    }

    public boolean run(final String input, final String baseOutput) throws Exception {
        final String output = baseOutput + OUTPUT_PATH;

        // set configurations
        final Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", OUTPUT_SEPARATOR); // set OUTPUT_SEPARATOR as separator

        // instantiate job
        final Job job = new Job(conf, "Count");
        job.setJarByClass(Count.class);

        // set mapper/combiner/reducer
        job.setMapperClass(CountMapper.class);
        job.setCombinerClass(CountReducer.class);
        job.setReducerClass(CountReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // define I/O
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // define input/output format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true);
    }

    public static void main(final String[] args) throws Exception {
        // set configurations
        final Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", OUTPUT_SEPARATOR); // set OUTPUT_SEPARATOR as separator

        // instantiate job
        final Job job = new Job(conf, "Count");
        job.setJarByClass(Count.class);

        // set mapper/combiner/reducer
        job.setMapperClass(CountMapper.class);
        job.setCombinerClass(CountReducer.class);
        job.setReducerClass(CountReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // define I/O
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // define input/output format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //job.setOutputFormatClass(SequenceFileOutputFormat.class);

        System.out.println(job.waitForCompletion(true));
    }
}
