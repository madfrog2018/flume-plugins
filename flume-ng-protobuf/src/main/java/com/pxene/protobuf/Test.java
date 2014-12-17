package com.pxene.protobuf;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class Test {
	private final static Logger logger = Logger.getLogger(Test.class);
    public static void main(String[] args) {
		Map<String, String> map = new HashMap<String, String>();
		String NULL = "NULL";
		String c = "\\|";
		int index = 0;
		int position = 0;
//		map.put("1", NULL);
//		logger.info(map.get("1"));
//		map.put("1", "122");
//		logger.info(map.get("1"));
		StringBuilder sb = new StringBuilder();
		sb.append("1111").append("|").append("2222").append("|").append("3333").append("|").append("4444");
		index = sb.indexOf("|", index);
		position = index -2;
		System.out.println(sb.indexOf("|", 0));
		sb.insert(position, ",aaaa");
		
		String ss = sb.toString();
		System.out.println(ss);
		String[] sss = ss.split(c);
		for (String string : sss) {
			System.out.println(string);
		}
	}
}
