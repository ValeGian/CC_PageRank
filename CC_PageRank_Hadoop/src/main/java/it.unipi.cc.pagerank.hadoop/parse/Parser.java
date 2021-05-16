package it.unipi.cc.pagerank.hadoop.parse;

import java.util.List;

public interface Parser {
    public void setStringToParse(final String stringToParse);

    public String getTitle();

    public List<String> getOutLinks();
}
