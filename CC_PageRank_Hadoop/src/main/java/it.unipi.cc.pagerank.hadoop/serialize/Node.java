package it.unipi.cc.pagerank.hadoop.serialize;

import com.google.gson.Gson;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Node implements WritableComparable<Node> {
    private double pageRank;
    private List<String> adjacencyList;

    //-------------------------------------------------------------------------------

    public Node() {
        setAdjacencyList(new ArrayList<String>());
    }

    public Node(final double pageRank, final List<String> adjacencyList) {
        set(pageRank, adjacencyList);
    }

    //-------------------------------------------------------------------------------

    public void setPageRank(final double pageRank) {
        this.pageRank = pageRank;
    }

    public void setAdjacencyList(final List<String> adjacencyList) {
        this.adjacencyList = adjacencyList;
    }

    public void set(final double pageRank, final List<String> adjacencyList) {
        setPageRank(pageRank);
        setAdjacencyList(adjacencyList);
    }

    public void setFromJson(final String json) {
        Node fromJson = new Gson().fromJson(json, Node.class);
        set(fromJson.getPageRank(), fromJson.getAdjacencyList());
    }

    public void addAdjNode(final String newAdjNode) {
        this.adjacencyList.add(newAdjNode);  // Deep copy
    }

    public double getPageRank() { return this.pageRank; }

    public List<String> getAdjacencyList() { return this.adjacencyList; }

    //-------------------------------------------------------------------------------

    public boolean isNode() { return ((this.adjacencyList != null) && (this.adjacencyList.size() > 0)); }

    //-------------------------------------------------------------------------------
    public void readFromText(String texts){
//        String s = "q6	{"pageRank":0.2,"adjacencyList":["q3","q4","q6"]}";

    }
    @Override
    public void write(DataOutput out) throws IOException {
        System.out.println("at the begining of the write");
        out.writeDouble(pageRank);

        out.writeInt(this.adjacencyList.size());
        for (String adjNode: this.adjacencyList) {
            out.writeUTF(adjNode);
        }
        System.out.println("at the end of the write");
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        System.out.println("i am in the beginning readfields");
        this.pageRank = in.readDouble();

        int size = in.readInt();
        this.adjacencyList = new ArrayList<String>();
        for (int i = 0; i < size; i++) {
            this.adjacencyList.add(in.readUTF());
        }
        System.out.println("at the end of the readfields");
    }

    //-------------------------------------------------------------------------------

    public String toHumanString() {
        return "[Rank: " + pageRank + "]\t[AdjList: " + adjacencyList + "]";
    }

    @Override
    public String toString() {
        System.out.println("test of toString of node");
        StringBuilder s = new StringBuilder("pageRank:" + Double.toString(this.pageRank) + '\t' + "adj:[");
        for (int i=0;i<this.adjacencyList.size();i++){
            s.append(this.adjacencyList.get(i));
            if (i!=(this.adjacencyList.size()-1)){
                s.append(",");
            }
        }
        s.append(']');
//        String json = new Gson().toJson(this);
//        return json;
        return s.toString();

    }

    public void fromString(String s) {


        String[] parts = s.split("\\t");


        //q6	pageRank:0.2	adj:[q3,q4,q6]
        if (parts.length > 2) {
            this.pageRank = Double.parseDouble(parts[1].split(":")[1]);

            String s1 = parts[2].split(":")[1];

            s1 = s1.replace("[", "");
            s1 = s1.replace("]", "");

            this.adjacencyList = new ArrayList<String>(Arrays.asList(s1.split(",")));
        }
        else{
            this.pageRank = Double.parseDouble(parts[0].split(":")[1]);

            String s1 = parts[1].split(":")[1];

            s1 = s1.replace("[", "");
            s1 = s1.replace("]", "");

            this.adjacencyList = new ArrayList<String>(Arrays.asList(s1.split(",")));
        }

    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (!(o instanceof Page)) {
            return false;
        }

        Node that = (Node) o;
        return that.getPageRank() == this.pageRank
                && that.getAdjacencyList().equals(this.adjacencyList);
    }

    @Override
    public int hashCode() {
        int hashCode = 17;
        hashCode = 31*hashCode + (int)this.pageRank;
        hashCode = 31*hashCode + (this.adjacencyList == null ? 0 : this.adjacencyList.hashCode());
        return hashCode;
    }

    @Override
    public int compareTo(Node that) {
        double thatRank = that.getPageRank();
        return this.pageRank < thatRank ? -1 : (this.pageRank == thatRank ? 0 : 1);
    }
}
