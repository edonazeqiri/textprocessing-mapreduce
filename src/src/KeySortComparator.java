

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeySortComparator extends WritableComparator {

	protected KeySortComparator() {
		super(CategoryWordChiPair.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CategoryWordChiPair key1 = (CategoryWordChiPair) w1;
		CategoryWordChiPair key2 = (CategoryWordChiPair) w2;

		int cmpResult = key1.getCategory().compareTo(key2.getCategory());
		if (cmpResult == 0)
		{
			return -Double.compare(Double.parseDouble(key1.getWordChiPair().split(",")[1]),
					(Double.parseDouble(key2.getWordChiPair().split(",")[1])));
		}
		return cmpResult;
	}
}