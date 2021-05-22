package it.unipi.cc.pagerank.hadoop;

import it.unipi.cc.pagerank.hadoop.parser.Parser;
import it.unipi.cc.pagerank.hadoop.parser.ParserWikiMicro;
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
    private static final String OUTPUT_PATH = "/parse";
    private String output;

    private static Parse instance = null;  // Singleton

    private Parse() { }

    public static Parse getInstance()
    {
        if (instance == null)
            instance = new Parse();

        return instance;
    }

    public String getOutputPath() { return output; }

    public static class ParseMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text reducerKey = new Text();
        private final Text reducerValue = new Text();
        private final Parser wiki_microParser = new ParserWikiMicro();

        // For each line of the input (web page), emit title and out-links
        @Override
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
        private int pageCount;
        private final Node outValue = new Node();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            this.pageCount = context.getConfiguration().getInt("page.count", 0);
        }

        // For each page, emit the title and its node features
        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            outValue.setPageRank(1.0d/this.pageCount);
            for (Text value: values) {
                outValue.addAdjNode(value.toString());
            }
            context.write(key, outValue);
        }
    }

    public boolean run(final String input, final String baseOutput, final int pageCount) throws Exception {
        this.output = baseOutput + OUTPUT_PATH;

        // set configurations
        final Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", "\t"); // set \t as separator

        // instantiate job
        final Job job = new Job(conf, "Parse");
        job.setJarByClass(Parse.class);

        // set mapper/reducer
        job.setMapperClass(ParseMapper.class);
        job.setReducerClass(ParseReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Node.class);

        // set page.count for initializing the ranks
        job.getConfiguration().setInt("page.count", pageCount);

        // define I/O
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(this.output));

        // define input/output format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true);
    }

    public static void main(final String[] args) throws Exception {
        // set configurations
        final Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        conf.set("mapreduce.output.textoutputformat.separator", "\t"); // set \t as separator

        // instantiate job
        final Job job = new Job(conf, "Parse");
        job.setJarByClass(Parse.class);

        // set mapper/reducer
        job.setMapperClass(ParseMapper.class);
        job.setReducerClass(ParseReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Node.class);

        // set page.count for initializing the ranks
        int pageCount = Count.getInstance().getPageCount(otherArgs[0], otherArgs[1]);
        job.getConfiguration().setInt("page.count", pageCount);

        // define I/O
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        // define input/output format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
