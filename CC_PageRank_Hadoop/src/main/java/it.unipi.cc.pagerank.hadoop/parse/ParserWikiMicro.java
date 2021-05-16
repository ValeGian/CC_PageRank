package it.unipi.cc.pagerank.hadoop.parse;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParserWikiMicro implements Parser {
    private String stringToParse;
    Pattern pattern;
    Matcher matcher;

    public ParserWikiMicro() {
    }

    @Override
    public void setStringToParse(String stringToParse) {
        this.stringToParse = stringToParse;
    }

    @Override
    public String getTitle() {
        this.pattern = Pattern.compile("<title.*?>(.*?)<\\/title>");
        this.matcher = this.pattern.matcher(this.stringToParse);
        if(matcher.find()) {
            return matcher.group(1).replace("\t", " "); //remove /t to be able to use it as a separator
        } else {
            return null;
        }
    }

    @Override
    public List<String> getOutLinks() {
        List<String> outLinks = new ArrayList<String>();
        this.pattern = Pattern.compile("\\[\\[(.*?)\\]\\]");
        this.matcher = this.pattern.matcher(this.stringToParse);
        while(matcher.find()) {
            outLinks.add(matcher.group(1).replace("\t", " ")); //remove /t to be able to use it as a separator
        }
        return outLinks;
    }
}
