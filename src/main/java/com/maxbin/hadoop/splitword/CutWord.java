package com.maxbin.hadoop.splitword;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;

public class CutWord extends Mapper<LongWritable, Text, NullWritable, Text>{
	
	private Set<String> expectedNature = new HashSet<String>() {{
		add("m");add("w");add("null");
    }};
    
    private static Set<String> wordSet = null;
    
    static {
    	
		try {
			wordSet = ReadStopWords.readWordFile();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }
    
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] fields = StringUtils.split(line, "\t");
		
		String sentence = fields[6];
		
		Result result = ToAnalysis.parse(sentence);
		List<Term> terms = result.getTerms();
		
		String cutedSentence = "";
		for(int i=0; i<terms.size(); i++) {
            String word = terms.get(i).getName(); 
            String natureStr = terms.get(i).getNatureStr();
            if(!word.equals("http") && !word.equals("t") && !word.equals("cn")){
	            if(!wordSet.contains(word) && !expectedNature.contains(natureStr)) {
	                cutedSentence += word + " ";
	            }
            }
        }
		
		context.write(NullWritable.get(), new Text(cutedSentence.trim()));
		
	}

}