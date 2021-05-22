package it.unipi.cc.pagerank.hadoop;

import it.unipi.cc.pagerank.hadoop.serialize.Node;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Rank {
    private static final String OUTPUT_PATH = "/rank";
    private String output;

    private static Rank instance = null;  // Singleton

    private Rank() { }

    public static Rank getInstance()
    {
        if (instance == null)
            instance = new Rank();

        return instance;
    }

    public String getOutputPath() { return output; }

    public static class RankMapper extends Mapper<Text, Text, Text, Node> {
        private final Text reducerKey = new Text();
        private final Node reducerValue = new Node();
        private static final List<String> empty = new ArrayList<String>();

        // For each line of the input (page title and its node features)
        // (1) emit page title and its node features to maintain the graph structure
        // (2) emit out-link pages with their mass (rank share)
        @Override
        public void map(final Text key, final Text value, final Context context) throws IOException, InterruptedException {
            reducerKey.set(key.toString());
            reducerValue.setFromJson(value.toString());
            context.write(reducerKey, reducerValue); // (1)

            List<String> outLinks = reducerValue.getAdjacencyList();
            final double mass = reducerValue.getPageRank() / outLinks.size();

            for(String outLink: outLinks) {
                reducerKey.set(outLink);
                reducerValue.set(mass, empty);
                context.write(reducerKey, reducerValue); // (2)
            }
        }
    }

    public static class RankReducer extends Reducer<Text, Node, Text, Node>{
        private double alpha;
        private double randomJumpFactor;
        private final Node outValue = new Node();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            this.alpha = context.getConfiguration().getDouble("alpha", 0);
            final int pageCount = context.getConfiguration().getInt("page.count", 0);
            randomJumpFactor = alpha / pageCount;
        }

        // For each node associated to a page
        // (1) if it is a complete node, recover the graph structure from it
        // (2) else, get from it an incoming rank contribution
        @Override
        public void reduce(Text key, Iterable<Node> values, Context context) throws IOException, InterruptedException {
            double rank = 0;

            for (Node p: values) {
                if(p.isCompleteNode())
                    outValue.set(p);  // (1)
                else
                    rank += p.getPageRank(); // (2)
            }
            double newPageRank = randomJumpFactor + ((1 - alpha) * rank);
            outValue.setPageRank(newPageRank);
            context.write(key, outValue);
        }

    }

    public boolean run(final String input, final String baseOutput, final double alpha, final int pageCount, final int iteration) throws Exception {
        this.output = baseOutput + OUTPUT_PATH + "-" + iteration;

        // set configurations
        final Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t"); // set \t as separator

        // instantiate job
        final Job job = new Job(conf, "Rank");
        job.setJarByClass(Rank.class);

        // set mapper/reducer
        job.setMapperClass(RankMapper.class);
        job.setReducerClass(RankReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Node.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Node.class);

        // set the random jump probability alpha and the page count
        job.getConfiguration().setDouble("alpha", alpha);
        job.getConfiguration().setInt("page.count", pageCount);

        // define I/O
        KeyValueTextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(this.output));

        // define input/output format
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true);
    }

    public static void main(final String[] args) throws Exception {
        // set configurations
        final Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t"); // set \t as separator

        // instantiate job
        final Job job = new Job(conf, "Rank");
        job.setJarByClass(Rank.class);

        // set mapper/reducer
        job.setMapperClass(RankMapper.class);
        job.setReducerClass(RankReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Node.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Node.class);

        // define I/O
        KeyValueTextInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // define input/output format
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
