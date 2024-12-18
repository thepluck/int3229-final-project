package job2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import driver.Driver;

import java.util.HashSet;
import java.util.Set;

public class Reducer2<T> extends MapReduceBase implements Reducer<Text, Text, Text,
Text>{
	private final Set<Integer> intersect = new HashSet<>();
    private final Set<Integer> union = new HashSet<>();
	
    Set<Integer> set1=new HashSet<Integer>();
    String user=Driver.arguments;
	String[] user_movies=user.split(",");
	int firstTime=1;
	
    public double compute(Set<Integer> set1, Set<Integer> set2)
    {
        intersect.clear();
        intersect.addAll(set1);
        intersect.retainAll(set2);
        union.clear();
        union.addAll(set1);
        union.addAll(set2);
        System.out.println("Set1 and Set2 --> "+set1.toString()+set2.toString());
        System.out.println("Union and intersection --> "+union.toString() + intersect.toString());
        return (double)intersect.size()/(double)union.size();
    }
    
    public void reduce(Text key, Iterator<Text> values,
	OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		
    	Set<Integer> set2=new HashSet<Integer>();
    	if(firstTime==1)
    	{
    		for (int i=0;i<user_movies.length;i++)
    		{
    			set1.add(Integer.parseInt(user_movies[i]));
    		}
    		firstTime=0;
    	}
    	while(values.hasNext())
    	{
    		String[] reducer=values.next().toString().split(",");
    		for(int i=0;i<reducer.length;i++)
    			System.out.println("REDUCER --> "+reducer[i]);
    		for (int i=0;i<reducer.length;i++)
    		{
    			set2.add(Integer.parseInt(reducer[i]));
    		}
    	}
		
    	double similarity=compute(set1, set2);
    	
		output.collect(key, new Text(Double.toString(similarity)));
	}

}
