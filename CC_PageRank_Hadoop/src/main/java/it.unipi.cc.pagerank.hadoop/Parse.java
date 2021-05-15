package it.unipi.cc.pagerank.hadoop;

import it.unipi.cc.pagerank.hadoop.serialize.GraphNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Parse {
    public static class ParseMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
            /*System.out.println("Map START - " + value.toString());
            Pattern p = Pattern.compile("\[[.*]]");
            Matcher m = p.matcher(value.toString());
            while (m.find())
            {
                String codeGroup = m.group();
                System.out.format("'%s'\n", codeGroup);
            }
            System.out.println("Map END\n\n");
             */
            context.write(new Text("Test Title"), new Text("Linked Page " + key.toString()));
        }
    }

    public static class ParseReducer extends Reducer<Text, Text, Text, GraphNode> {
        private final Text reducerKey = new Text();

        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            GraphNode reducerOutValue = new GraphNode();
            reducerOutValue.setPageRank(2.0);
            for (Text value: values) {
             reducerOutValue.addAdjNode(value);
             System.out.println(value.toString());
            }
            reducerKey.set(key.toString()+"]]");
            context.write(reducerKey, reducerOutValue);
        }
    }

    public static class TestParseMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
            GraphNode node = new GraphNode();
            String record = value.toString();

            int endKey = record.indexOf("]]");
            String keyPage = record.substring(0, endKey);

            int startValue = record.indexOf("{");
            node.setFromJson(record.substring(startValue));

            System.out.println(keyPage);
            System.out.println(node.toPrintableString());
        }
    }

    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();
        final Job job = new Job(conf, "Parse");
        job.setJarByClass(PageRank.class);

        // set mapper/reducer
        //job.setMapperClass(ParseMapper.class);
        job.setMapperClass(TestParseMapper.class);
        job.setReducerClass(ParseReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(GraphNode.class);

        // define I/O
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
