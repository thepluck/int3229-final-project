package driver;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;

import job1.Mapper1;
import job1.Reducer1;
import job2.Mapper2;
import job2.Reducer2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;


public class Driver {
	public static String arguments;
	public Set<String> getKeysByValue(Map<String, Double> map, double value) {
	    Set<String> keys = new HashSet<String>();
	    for (Map.Entry<String, Double> entry : map.entrySet()) {
	        if (value == entry.getValue()) {
	            keys.add(entry.getKey());
	        }
	    }
	    return keys;
	}

	public static void main(String[] args) throws IOException {
				/*
				 * Read the centroid file into a Hashmap
				 */
				arguments="242,51,86";
				/*
				 * 	Job 1: Gathering each user's list of rated movies
				 */
					JobClient client1 = new JobClient();
					JobConf conf1 = new JobConf(Driver.class);
					
					conf1.setMapperClass(Mapper1.class);
					conf1.setReducerClass(Reducer1.class);
					conf1.setMapOutputKeyClass(Text.class);
					conf1.setMapOutputValueClass(Text.class);
					conf1.setOutputKeyClass(Text.class);
					conf1.setOutputValueClass(Text.class);
						
					String input, output;	
							
					input = "./hadoop/input/";	
						
					output = "./hadoop/output";		
					FileInputFormat.setInputPaths(conf1, new Path(input));
					FileOutputFormat.setOutputPath(conf1, new Path(output));
					client1.setConf(conf1);
							
					try {
						JobClient.runJob(conf1);
					} 
					catch (Exception e) {
					e.printStackTrace();
					}
					
					
					JobClient client2 = new JobClient();
					JobConf conf2 = new JobConf(Driver.class);
					
					conf2.setMapperClass(Mapper2.class);
					conf2.setReducerClass(Reducer2.class);
					conf2.setMapOutputKeyClass(Text.class);
					conf2.setMapOutputValueClass(Text.class);
					conf2.setOutputKeyClass(Text.class);
					conf2.setOutputValueClass(Text.class);
						
					String input1, output1;	
							
					input1 = "./hadoop/output/part-00000";	
						
					output1 = "./hadoop/output1";		
					FileInputFormat.setInputPaths(conf2, new Path(input1));
					FileOutputFormat.setOutputPath(conf2, new Path(output1));
					client2.setConf(conf2);
							
					try {
						JobClient.runJob(conf2);
					} 
					catch (Exception e) {
					e.printStackTrace();
					}
					
					try{
						Path pt=new Path("./hadoop/output1/part-00000");
				        FileSystem fs = FileSystem.get(new Configuration());
				        BufferedReader bufferReader = new BufferedReader(new InputStreamReader(fs.open(pt)));
				        String line;
				        LinkedHashMap<String, Double> similarity = new LinkedHashMap<String, Double>();
			
				        while ((line = bufferReader.readLine()) != null)   {
				        	String[] temp = line.split("\t");
				        	double value = Double.parseDouble(temp[1]);
				        	similarity.put(temp[0], value);
				        }
				        bufferReader.close();
				        
				        LinkedHashSet<Double> values = new LinkedHashSet<Double>();
				        for (Map.Entry<String, Double> entry : similarity.entrySet()) {
				            values.add(entry.getValue());
				            System.out.println(entry.getKey()+": "+entry.getValue());
				        }
				        
				        ArrayList<Double> valueF = new ArrayList<Double>();
				        for(double temp: values){
				        	valueF.add(temp);
				        }
				        Collections.sort(valueF);
				        Collections.reverse(valueF);
				        
				        int count = 0;
				        
				        Path pt2=new Path("./hadoop/final/final.txt");
						FileSystem fs1 = FileSystem.get(new Configuration());
						FSDataOutputStream fsOutStream = fs1.create(pt2, true);
						BufferedWriter br1 = new BufferedWriter( new OutputStreamWriter(fsOutStream, "UTF-8" ) );
				        
				        Driver obj = new Driver();
				        for(double temp: valueF){
				        	Set<String> tempKeys = obj.getKeysByValue(similarity, temp);
				        	if(!tempKeys.isEmpty()){
				        		for(String temp1: tempKeys){
				        		if(count == 100) {
				        			br1.close();
				        			System.exit(0);
				        		}
				        		System.out.println("Final output- "+temp1+": "+temp );
				        		br1.write(temp1+"\t"+temp+"\n");
				        		count++;
				        		}
				        	}
				        	
				        }
				        
				    }
				    catch(Exception e){
				    	System.out.println("Error while reading file line by line:" + e.getMessage());                      
				    }
	}

}
