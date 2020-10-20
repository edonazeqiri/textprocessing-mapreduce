

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator {
  protected GroupComparator() {
		super(CategoryWordChiPair.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CategoryWordChiPair key1 = (CategoryWordChiPair) w1;
		CategoryWordChiPair key2 = (CategoryWordChiPair) w2;
		return key1.getCategory().compareTo(key2.getCategory());
	}
}
