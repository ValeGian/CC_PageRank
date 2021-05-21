package it.unipi.cc.pagerank.hadoop;

//import it.unipi.cc.pagerank.hadoop.serialize.GraphNode;
import it.unipi.cc.pagerank.hadoop.serialize.Node;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.net.unix.DomainSocket;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Rank {

    public static class RankMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text reducerKey = new Text();
        private final Text reducerGraphValue = new Text();


        @Override
        public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
//            System.out.println("mapper works or not.");
            Node pageNode = new Node();
//            System.out.println("mapper works or not..");
            pageNode.fromString(value.toString());
//            System.out.println("mapper works or not!");
            String s = value.toString();
//            System.out.println("mapper works or not!!");
            System.out.println(s);
//            System.out.println("mapper works or not!!!");
            String[] arr = s.split("\\t");

            reducerKey.set(arr[0]);
            reducerGraphValue.set(pageNode.toString());
            context.write(reducerKey, reducerGraphValue);
           // context.write(new Text("doros"),new Text("sho"));

            for(int i = 0; i < pageNode.getAdjacencyList().size(); i++) {
                double edgeRange = pageNode.getPageRank() / pageNode.getAdjacencyList().size();
                String edge = "edgeRank:" + Double.toString(edgeRange);
                context.write(new Text(pageNode.getAdjacencyList().get(i)), new Text(edge));
            }
//            String[] ads = arr[1].split(",");
//            DoubleWritable rank = new DoubleWritable(0.2);
//            List<String> adjList = new ArrayList<String>();
//
//

            //reducerGraphValue.set(rank.get(), adjList);

        }
    }
    public static class RankReducer extends Reducer<Text, Text, Text, Text>{

        private final Node result = new Node();



        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException,  InterruptedException  {
            System.out.println("tessssst final reduce");
            Node pageNode = new Node();
            double pvalue = 0;
//            System.out.println("im here from reduce");
            for ( Text val  : values) {
                String string = val.toString();
                System.out.println(string);

                boolean contains = string.contains("pageRank");
                System.out.println(contains);
                if (contains){
                    pageNode.fromString(string);
                }
                else {
                    pvalue += Double.parseDouble(string.split(":")[1]);
                }
            }
            System.out.println("tessssst final");
            pageNode.setPageRank(pvalue);
            Text finalVal = new Text(pageNode.toString());
            context.write(key, finalVal);
        }

    }


    public static void main(final String[] args) throws Exception {
//        final Configuration conf = new Configuration();
//        final Job job = new Job(conf, "PageRank");
//        job.setJarByClass(Rank.class);
//        job.setJarByClass( this .getClass());
//
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//
//        job.setMapperClass(RankMapper.class);
//        job.setReducerClass(RankReducer.class);
//
//        job.setMapOutputValueClass(Text.class);
//
//        job.setOutputValueClass(Text.class);
//
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//
//        job.setInputFormatClass(TextInputFormat.class);
//
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
        final Configuration conf = new Configuration();
        final Job job = new Job(conf, "PageRank");
        job.setJarByClass( Rank.class);

        job.setOutputKeyClass( Text .class);
        job.setOutputValueClass( Text .class);
        job.setMapperClass( RankMapper .class);

        FileInputFormat.addInputPath(job,  new Path(args[0]));
        FileOutputFormat.setOutputPath(job,  new Path(args[1]));
        job.setReducerClass(RankReducer .class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


/*public class Rank {
    public static class RankMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(final LongWritable key, final Text value, final Context context)
                throws IOException,  InterruptedException {
            int pageTableIndex = value.find("[");
            int rankTableIndex = value.find("[",pageTableIndex+1);

        }
    }
    public static class RankReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(final Text key, final Iterable<Text> values, final Context context)
                throws IOException,  InterruptedException{

        }
    }

}*/
