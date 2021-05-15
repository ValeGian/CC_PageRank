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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Parse {
    public static class ParseMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text reducerKey = new Text();
        private final Text reducerValue = new Text();

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
            reducerKey.set("Test Page Title");
            reducerValue.set("Page " + key.toString());
            System.out.println(">> Map writes [Page] \"" + reducerKey + "\t[Linked Page] \"" + reducerValue + "\"");
            context.write(reducerKey, reducerValue);
        }
    }

    public static class ParseReducer extends Reducer<Text, Text, Text, GraphNode> {
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            GraphNode reducerOutValue = new GraphNode();
            reducerOutValue.setPageRank(2.0);
            for (Text value: values) {
             reducerOutValue.addAdjNode(value);
            }
            context.write(key, reducerOutValue);
        }
    }

    public static class TestRankMapper extends Mapper<Text, Text, Text, Text> {
        public void map(final Text key, final Text value, final Context context) throws IOException, InterruptedException {
            GraphNode node = new GraphNode();
            node.setFromJson(value.toString());

            System.out.println(key);
            System.out.println(node.toPrintableString());
        }
    }

    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", "]"); // changes output separator from \t to ]
        //conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "]"); // changes input separator from \t to ]
        final Job job = new Job(conf, "Parse");
        job.setJarByClass(PageRank.class);

        // set mapper/reducer
        job.setMapperClass(ParseMapper.class);
        //job.setMapperClass(TestRankMapper.class);
        job.setReducerClass(ParseReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(GraphNode.class);

        // define I/O
        //KeyValueTextInputFormat.addInputPath(job, new Path(args[0])); // for TestRankMapper
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //job.setInputFormatClass(KeyValueTextInputFormat.class); // for TestRankMapper
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
