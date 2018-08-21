package work5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class IntPair implements WritableComparable<IntPair> {
	private int first = 0;
	private double second = 0;

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		first = in.readInt();
	    second = in.readDouble();
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(first);
	    out.writeDouble(second);
	}

	public int compareTo(IntPair o) {
		// TODO Auto-generated method stub
		if (first != o.first) {
		      if(first > o.first)
		    	  return 1;
		      else 
		    	  return -1;
		    } else if (second != o.second) {
		    	if(second > o.second)
			    	  return -1;
			      else 
			    	  return 1;
		    } else {
		      return 0;
		    }
	}

	public void set(int left, double right) {
	    first = left;
	    second = right;
	  }
	
	public int getFirst() {
	    return first;
	  }
	public double getSecond() {
	    return second;
	  }
	
	  @Override
	  public boolean equals(Object right) {
	    if (right instanceof IntPair) {
	      IntPair r = (IntPair) right;
	      return r.first == first && r.second == second;
	    } else {
	      return false;
	    }
	  }
}
