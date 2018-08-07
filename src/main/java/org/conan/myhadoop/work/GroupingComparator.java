package org.conan.myhadoop.work;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator implements RawComparator<IntPair> {
	  @Override
	  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	    return WritableComparator.compareBytes(b1, s1, Integer.SIZE/8, b2, s2, Integer.SIZE/8);
	  }

	  @Override
	  public int compare(IntPair o1, IntPair o2) {
	    double first1 = o1.getFirst();
	    double first2 = o2.getFirst();
	    if(first1 > first2)
	    	  return 1;
	    else if(first1 < first2)
	    	  return -1;
	    else
	    	  return 0;
	  }
}
