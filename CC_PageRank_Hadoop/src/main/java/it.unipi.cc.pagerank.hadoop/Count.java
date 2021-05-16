package it.unipi.cc.pagerank.hadoop;

import it.unipi.cc.pagerank.hadoop.serialize.GraphNode;
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
    private String outputDirectory = "output"; //to be initialized when Count will be called in the pipeline

    public static class CountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final Text keyEmit = new Text(OUTPUT_KEY);

        public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
            context.write(keyEmit, one);
        }
    }

    public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (final IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public int getPageCount() throws IOException {
        final String file = outputDirectory + OUTPUT_PATH + "/part-r-00000";

        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = reader.readLine();
        reader.close();

        String[] tokens = line.trim().split(OUTPUT_SEPARATOR);
        assertEquals(OUTPUT_KEY, tokens[0]);

        return Integer.parseInt(tokens[1]);
    }

    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", OUTPUT_SEPARATOR); // changes output separator from \t to -
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

        if(job.waitForCompletion(true)) {
            System.out.println(new Count().getPageCount());
        } else {
            System.out.println(job.getJobName() + "Job did not succeed");
        }

    }
}
