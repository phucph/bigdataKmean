import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;
//import java.util.StringTokenizer;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;


public class KMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text,TwoDPointWritable > {
	private final static IntWritable K = new IntWritable(2);
	private final static TwoDPointWritable point = new TwoDPointWritable();
	public double [][] centroids = new double [K.get()][2];
	public final static String centerfile="C:\\Users\\hongphuc\\eclipe\\Kmean\\input\\Point.txt";
	public void map(LongWritable key, Text value, OutputCollector<Text, TwoDPointWritable> output ,Reporter reporter ) throws IOException {
		Scanner reader = new Scanner(new FileReader(centerfile));

		  for (int  i=0; i<K.get(); i++ ) {
		      centroids[i][0] = reader.nextDouble();
		      centroids[i][1] = reader.nextDouble();
		  }
		String valueString = value.toString();
//		System.out.println(valueString);
		String[] SingleCountryData = valueString.split(" ");
			StringTokenizer itr = new StringTokenizer(SingleCountryData[0].toString());
			StringTokenizer itrs = new StringTokenizer(SingleCountryData[1].toString());
//			System.out.println(centroids[0][0]+ "-"+centroids[0][1]+"/"+centroids[1][0]+ "-"+centroids[1][1]);
		while (itr.hasMoreTokens()) {
		double distance = 0;
		double mindistance = 99999999.9d;
		int choseCentroids =-1;
		int j=0;
		double x =  Double.parseDouble(itr.nextToken());
		double y =  Double.parseDouble(itrs.nextToken());
		point.set(x, y);
		while (j<K.get()) {
			distance = ( x-centroids[j][0])*(x-centroids[j][0]) + 
					  (y - centroids[j][1])*(y-centroids[j][1]);
			
				      if ( distance < mindistance ) {
					  mindistance = distance;
					  choseCentroids=j;
				      }
			j++;
		}
		String index = String.format("%d", choseCentroids);
		Text cluster = new Text(index);
		output.collect(cluster, point);	
		System.out.println(cluster +"//"+ point.getx().toString()+","+point.gety().toString());
		}
	}
}
