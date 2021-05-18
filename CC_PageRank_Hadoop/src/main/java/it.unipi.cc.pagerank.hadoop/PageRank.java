package it.unipi.cc.pagerank.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import it.unipi.cc.pagerank.hadoop.serialize.Node;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank {
    /*
    public static class NewMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {
            final StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class NewReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(final Text key, final Iterable<IntWritable> values, final Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (final IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
*/
    public static class NewMapper extends Mapper<LongWritable, Text, Text, Node>
    {
        private final Text reducerKey = new Text();
        private final Node reducerGraphValue = new Node();

        public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
            DoubleWritable rank = new DoubleWritable(2.0);
            List<String> adjList = new ArrayList<String>();
            for(int i = 0; i < 3; i++) {
                adjList.add("prova");
            }

            reducerKey.set("Here");
            reducerGraphValue.set(rank.get(), adjList);
            context.write(reducerKey, reducerGraphValue);
        }
    }

    public static class NewReducer extends Reducer<Text, Node, Text, Node> {
        private final Node result = new Node();

        public void reduce(final Text key, final Iterable<Node> values, final Context context) throws IOException, InterruptedException {
            Node result = values.iterator().next();
            context.write(key, result);
        }
    }

    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();
        final Job job = new Job(conf, "PageRank");
        job.setJarByClass(PageRank.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(NewMapper.class);
        job.setReducerClass(NewReducer.class);

        job.setMapOutputValueClass(Node.class);

        job.setOutputValueClass(Node.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(TextInputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
