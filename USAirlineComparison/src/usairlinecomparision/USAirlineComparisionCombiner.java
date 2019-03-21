package org.usairlinecomparision;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class USAirlineComparisionCombiner extends Reducer<Text, Text, Text, Text> {
	public Text Pair = new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String main = "";
		for(Text value : values) 
		{
			if(main=="") 
			{
				main = value.toString();
			}
			else 
			{
				String[] sub = main.split(";");
				boolean found = false;
				String[] input = value.toString().split(",");
				String temp1="";
				
				for(String set : sub) 
				{
					String[] input1 = set.toString().split(",");
					if(input1[0].trim().equalsIgnoreCase(input[0].trim()))
					{
						found = true;
						set = input1[0] + "," + String.valueOf(Integer.parseInt(input1[1].trim()) + Integer.parseInt(input[1].trim())) + "," + String.valueOf(Integer.parseInt(input1[2].trim()) 
								+ Integer.parseInt(input[2].trim())) + "," + String.valueOf(Integer.parseInt(input1[3].trim()) + Integer.parseInt(input[3].trim()));
						
					}
					if(temp1.trim().equalsIgnoreCase(""))
					{
						temp1 = set;
					}
					else
					{
						temp1 = temp1 + ";" + set ;
					}
				}
				
				if(!found) 
				{
					main = temp1 + ";" + value;
				}else {
					main = temp1;
				}
			}
			
		}
		
		Pair.set(main);
		context.write(key, Pair);
		
		
	}

}
