import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import sun.awt.SunHints.Value;

public class KReducer extends MapReduceBase implements Reducer<Text, TwoDPointWritable, Text, Text> {

	public void reduce(Text t_key, Iterator<TwoDPointWritable> values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
		
		int num = 0;
		  double centerx=0.0d;
		  double centery=0.0d;  
		  while (values.hasNext()){
			  num++;
			  TwoDPointWritable index =  values.next();
			  DoubleWritable X = index.getx();
		      double x = X.get();
			  DoubleWritable Y = index.gety();
		      double y = Y.get();
		      centery += y;
		      centerx += x;
		  }
		  centerx = centerx/num;
		  centery = centery/num;
//		  String index = String.format("%d", t_key);
//		  String preres = String.format("%d %d", centerx, centery);
//		  Text result = new Text(preres);
		output.collect(t_key,new Text(centerx+","+centery) );
	}
	}
	
