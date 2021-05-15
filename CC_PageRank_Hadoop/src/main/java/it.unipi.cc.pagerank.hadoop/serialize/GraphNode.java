package it.unipi.cc.pagerank.hadoop.serialize;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GraphNode implements Writable {
    private DoubleWritable pageRank;
    private List<Text> adjacencyList;

    //-------------------------------------------------------------------------------

    public GraphNode() {
    }

    public GraphNode(final DoubleWritable pageRank, final List<Text> adjacencyList) {
        setPageRank(pageRank);
        setAdjacencyList(adjacencyList);
    }

    //-------------------------------------------------------------------------------

    public void setPageRank(DoubleWritable pageRank) {
        this.pageRank = pageRank;
    }

    public void setAdjacencyList(List<Text> adjacencyList) {
        this.adjacencyList = adjacencyList;
    }

    public void set(DoubleWritable pageRank, List<Text> adjacencyList) {
        setPageRank(pageRank);
        setAdjacencyList(adjacencyList);
    }

    public DoubleWritable getPageRank() { return this.pageRank; }

    public List<Text> getAdjacencyList() { return this.adjacencyList; }

    //-------------------------------------------------------------------------------

    @Override
    public void write(DataOutput out) throws IOException {
        this.pageRank.write(out);

        out.writeInt(this.adjacencyList.size());
        for (Text adjNode: this.adjacencyList) {
            adjNode.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.pageRank = new DoubleWritable(in.readDouble());

        int size = in.readInt();
        this.adjacencyList = new ArrayList<Text>();
        Text tmp = new Text();
        for (int i = 0; i < size; i++) {
            tmp.readFields(in);
            this.adjacencyList.add(tmp);
        }
    }

    //-------------------------------------------------------------------------------

    public String toString() {
        return "[Rank: " + this.pageRank + "]\t[Adj List: " + this.adjacencyList + "]";
    }
}
