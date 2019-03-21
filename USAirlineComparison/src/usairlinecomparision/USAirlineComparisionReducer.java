package org.usairlinecomparision;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class USAirlineComparisionReducer extends Reducer<Text, Text, Text, Text> {
	public Text Pair = new Text();
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String main = "";
		HashMap<String,String> map = new HashMap<String,String>();

		for(Text value : values) {
			main = value.toString();
			String[] subStr = main.split(";");
			for (String subStr1 : subStr) {
				String[] set = subStr1.split(",");
				int sum = Integer.parseInt(set[1])+Integer.parseInt(set[2])+Integer.parseInt(set[3]);
				int percent = (Integer.parseInt(set[1])*100)/sum;
				map.put(set[0],String.valueOf(percent));
			}
			int maxValue = Integer.parseInt(Collections.max(map.values()));
			for(Entry<String,String> entry : map.entrySet()){
				if(entry.getValue().equals(String.valueOf(maxValue))){
					Pair.set(entry.getKey() + ":" + entry.getValue());
				}
			}
		}
		context.write(key, Pair);


	}

}
