package it.unipi.cc.pagerank.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

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
    public static class NewMapper extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private final Text reducerKey = new Text();

        public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
            int sum = 0;
            String record = value.toString();
            if (record == null || record.length() == 0)
                return;

            String[] tokens = record.trim().split("</title>");
            for(String token: tokens) {
                sum += 1;
            }
            reducerKey.set(tokens[0]);
            //reducerKey.set("prova"); //to count how many lines (pages) there are
            context.write(reducerKey, new IntWritable(sum));
        }
    }

    public static class NewReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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

    public static void main(final String[] args) throws Exception {
        //System.setProperty("hadoop.home.dir", "C:/hadoop");
        final Configuration conf = new Configuration();
        final Job job = new Job(conf, "PageRank");
        job.setJarByClass(PageRank.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(NewMapper.class);
        job.setReducerClass(NewReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(TextInputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
