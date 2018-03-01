package com.maxbin.hadoop.splitword;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;

public class TestDemo {
	
	private static Set<String> readWordFile(){
		
		Set<String> wordSet = null;
		
		File file = new File("E:/Kaggle/tianchi/data/stopWord.txt");
		
		try {
			InputStreamReader read = new InputStreamReader(new FileInputStream(file), "utf-8");
			
			if (file.isFile() && file.exists()) {
				wordSet = new HashSet<String>();
				BufferedReader br = new BufferedReader(read);
				String txt = null;
				
				while ((txt = br.readLine()) != null) {
					wordSet.add(txt);
				}
				
				br.close();
			}
			read.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return wordSet;
		
	}
	
	public static void main(String[] args) {
		
//		Set<String> expectedNature = new HashSet<String>() {{
//            add("n");add("v");add("vd");add("vn");add("vf");
//            add("vx");add("vi");add("vl");add("vg");
//            add("nt");add("nz");add("nw");add("nl");
//            add("ng");add("userDefine");add("wh");add("ns");
//        }};
		
		Set<String> expectedNature = new HashSet<String>() {{
			add("m");add("w");add("null");
        }};
		
		String sentence = "丽江旅游(sz002033)#股票##炒股##财经##理财##投资#推荐包赢股，~%&*(具体地说盈利对半分成，不算本金的啊，群：46251412 http://t.cn/zWoY9IZ";
		Result result = ToAnalysis.parse(sentence);
		System.out.println(result.getTerms());
		
		List<Term> terms = result.getTerms();
		
		for(int i=0; i<terms.size(); i++) {
            String word = terms.get(i).getName(); 
            String natureStr = terms.get(i).getNatureStr();
            if(!word.equals("http") && !word.equals("t") && !word.equals("cn")){
	            if(!readWordFile().contains(word) && !expectedNature.contains(natureStr)) {
	                System.out.println(word + ":" + natureStr);
	            }
            }
        }
		
	}

}
