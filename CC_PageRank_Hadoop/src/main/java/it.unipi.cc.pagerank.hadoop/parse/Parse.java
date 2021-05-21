package it.unipi.cc.pagerank.hadoop.parse;

import it.unipi.cc.pagerank.hadoop.count.Count;
import it.unipi.cc.pagerank.hadoop.serialize.Node;
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
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.List;

public class Parse {
    public static class ParseMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text reducerKey = new Text();
        private final Text reducerValue = new Text();
        private final Parser wiki_microParser = new ParserWikiMicro();

        public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
            String record = value.toString();
            wiki_microParser.setStringToParse(record);
            String title = wiki_microParser.getTitle();
            List<String> outLinks = wiki_microParser.getOutLinks();

            if(title != null && outLinks.size() > 0) {
                reducerKey.set(title);
                for(String outLink: outLinks) {
                    reducerValue.set(outLink);
                    context.write(reducerKey, reducerValue);
                }
            }
        }
    }

    public static class ParseReducer extends Reducer<Text, Text, Text, Node> {
        private int pageCount = 10;

        public void setup(Context context) throws IOException, InterruptedException {
//            this.pageCount = context.getConfiguration().getInt("page.count", 0);
        }

        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            Node reducerOutValue = new Node();
            reducerOutValue.setPageRank(1.0d/this.pageCount);
            for (Text value: values) {
                reducerOutValue.addAdjNode(value.toString());
            }
            context.write(key, reducerOutValue);
        }
    }

    public static class TestRankMapper extends Mapper<Text, Text, Text, Text> {
        public void map(final Text key, final Text value, final Context context) throws IOException, InterruptedException {
            System.out.println("Map - " + value.toString());
            Node node = new Node();
            node.setFromJson(value.toString());

            System.out.println(key);
            System.out.println(node.toHumanString());
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

        conf.set("mapreduce.output.textoutputformat.separator", "\t"); // changes output separator from \t to </title>
        //conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t"); // changes input separator from \t to </title>

        // instantiate job
        final Job job = new Job(conf, "Parse");
        job.setJarByClass(Parse.class);

        // set mapper/reducer
        //job.setMapperClass(TestRankMapper.class);
        job.setMapperClass(ParseMapper.class);
        job.setReducerClass(ParseReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Node.class);

        // set page.count for initializing the ranks
//        Count countStage = new Count();
//        int pageCount = countStage.getPageCount();
//        job.getConfiguration().setInt("page.count", pageCount);

        // define I/O
        //KeyValueTextInputFormat.addInputPath(job, new Path(args[0])); // for TestRankMapper
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        // define input/output format
        //job.setInputFormatClass(KeyValueTextInputFormat.class); // for TestRankMapper
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.out.println("Main");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
