package it.unipi.cc.pagerank.hadoop.sort;

import it.unipi.cc.pagerank.hadoop.serialize.Node;
import it.unipi.cc.pagerank.hadoop.serialize.Page;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class Sort {
    public static class SortMapper extends Mapper<Text, Text, Page, NullWritable> {
        public void map(final Text key, final Text value, final Context context) throws IOException, InterruptedException {
            Node node = new Node();
            node.setFromJson(value.toString());
            Page page = new Page(key.toString(), node.getPageRank());
            context.write(page, NullWritable.get());
        }
    }

    public static class SortReducer extends Reducer<Page, NullWritable, Text, DoubleWritable> {
        private final Text title = new Text();
        private final DoubleWritable rank = new DoubleWritable();

        public void reduce(final Page key, final Iterable<NullWritable> values, final Context context) throws IOException, InterruptedException {
            title.set(key.getTitle());
            rank.set(key.getRank());
            context.write(title, rank);
        }
    }

    public static void main(final String[] args) throws Exception {
        // set configuration
        final Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        /*if (otherArgs.length != 3) {
            System.err.println("Usage: MovingAverage <count_input> <input> <output>");
            System.exit(1);
        }
        System.out.println("args[0]: <count_input>="+otherArgs[0]);
        System.out.println("args[1]: <input>="+otherArgs[1]);
        System.out.println("args[2]: <output>="+otherArgs[2]);
         */

        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t"); // changes input separator from \t to </title>

        // instantiate job
        final Job job = new Job(conf, "Sort");
        job.setJarByClass(Sort.class);

        // set mapper/reducer
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(Page.class);
        job.setMapOutputValueClass(NullWritable.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // define I/O
        KeyValueTextInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        // define input/output format
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
