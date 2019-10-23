
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;


public class TwoDPointWritable implements Writable {

    private DoubleWritable x,y;	

    public TwoDPointWritable() {
	this.x = new DoubleWritable();
	this.y = new DoubleWritable();		
    }
	
    public void set ( double a, double b)
    {
	this.x.set(a);
	this.y.set(b);	
    }
	
    
    @Override
    public void readFields(DataInput in) throws IOException {
	x.readFields(in);
	y.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
	x.write(out);
	y.write(out);
    }
	
	
    public DoubleWritable getx() {
	return x;
    }

    public DoubleWritable gety() {
	return y;
    }

}