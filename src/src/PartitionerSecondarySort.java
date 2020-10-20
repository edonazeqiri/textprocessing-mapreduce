



import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerSecondarySort extends
  	Partitioner<CategoryWordChiPair, NullWritable> {

	@Override
	public int getPartition(CategoryWordChiPair key, NullWritable value,
			int numReduceTasks) {

		return (key.getCategory().hashCode() % numReduceTasks);
	}
}