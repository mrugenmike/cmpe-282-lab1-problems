package com.tfidf.stage1;

import java.util.HashSet;
import java.util.Set;

public class StopWords {
    private final Set<String> googleStopwords;
  public StopWords(){
      googleStopwords = new HashSet<String>();
      googleStopwords.add("I");
      googleStopwords.add("a");
      googleStopwords.add("about");
      googleStopwords.add("an");
      googleStopwords.add("are");
      googleStopwords.add("as");
      googleStopwords.add("at");
      googleStopwords.add("be");
      googleStopwords.add("by");
      googleStopwords.add("com");
      googleStopwords.add("de");
      googleStopwords.add("en");
      googleStopwords.add("for");
      googleStopwords.add("from");
      googleStopwords.add("how");
      googleStopwords.add("in");
      googleStopwords.add("is");
      googleStopwords.add("it");
      googleStopwords.add("la");
      googleStopwords.add("of");
      googleStopwords.add("on");
      googleStopwords.add("or");
      googleStopwords.add("that");
      googleStopwords.add("the");
      googleStopwords.add("this");
      googleStopwords.add("to");
      googleStopwords.add("was");
      googleStopwords.add("what");
      googleStopwords.add("when");
      googleStopwords.add("where");
      googleStopwords.add("who");
      googleStopwords.add("will");
      googleStopwords.add("with");
      googleStopwords.add("and");
      googleStopwords.add("the");
      googleStopwords.add("www");
  }


    boolean isStopWord(String word){
        return googleStopwords.contains(word);
    }
}
