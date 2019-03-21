package org.usairlinecomparision;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class USAirlineComparisionMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Logger logger = Logger.getLogger(USAirlineComparisionMapper.class);

	public static final String negativePath = "D:/MSc Data/High performance computing/negativewords.txt";
	public static final String positivePath = "D:/MSc Data/High performance computing/positivewords.txt";

	public static final Text tempSentiments = new Text();
	private Text timeZone = new Text();

	ArrayList<String> listNegative = new ArrayList<String>();
	ArrayList<String> listPositive = new ArrayList<String>();

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		try {
			Scanner sNegative = new Scanner(new File(negativePath));
			Scanner sPositive = new Scanner(new File(positivePath));

			while (sNegative.hasNextLine()){
				listNegative.add(sNegative.nextLine());
			}
			while(sPositive.hasNextLine()) {
				listPositive.add(sPositive.nextLine());
			}
			sNegative.close();
			sPositive.close();
		}catch(FileNotFoundException e) {
			e.printStackTrace();
		}


	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		int positiveFeedback = 0;
		int negativeFeedback = 0;
		double result = 0;

		String[] row = value.toString().split("\t");
		String tweet = "";
		if(row.length == 5) {
			logger.info("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
			for(int i = 0; i < row.length; i++) {
				logger.info("row["+i+"] : " + row[i]);
			}
			logger.info("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
			if(row[3] != "") {
				tweet = row[3];
			}

			logger.info("#########################");
			logger.info("array size: " +row.length);
			logger.info("#########################");

			if(!row[4].equals(" ") || !row[4].equals("")) 
			{
				timeZone.set(row[4]);
			}else {
				timeZone.set("Unknown Location : ");
			}

			String[] splitedTweet = tweet.split(" ");
			for(int i = 0; i < splitedTweet.length; i++) {
				if(listNegative.contains(splitedTweet[i])) {
					negativeFeedback++;
				}else if(listPositive.contains(splitedTweet[i])) {
					positiveFeedback++;
				}
			}

			try {
				result = (positiveFeedback + negativeFeedback)/(positiveFeedback - negativeFeedback);
			}catch(ArithmeticException e) {
				tempSentiments.set(row[1]+",0,0,1"); //Airline,negative,positive,neutral
			}

			if(result == 0) {
				tempSentiments.set(row[1]+",0,0,1");
			}else if(result > 0) {
				tempSentiments.set(row[1]+",0,1,0");
			}else {
				tempSentiments.set(row[1]+",1,0,0");
			}
		}
		context.write(timeZone, tempSentiments);


	}



}
