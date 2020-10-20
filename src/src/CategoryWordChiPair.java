import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CategoryWordChiPair implements Writable, WritableComparable<CategoryWordChiPair> {

	private String category;
	private String wordChiPair;

	public CategoryWordChiPair() {
	}

	public CategoryWordChiPair(String category, String wordChiPair) {
		this.category = category;
		this.wordChiPair = wordChiPair;
	}

	@Override
	public String toString() {
		return (new StringBuilder().append(category).append("\t").append(wordChiPair)).toString();
	}

	public void readFields(DataInput dataInput) throws IOException {
		category = WritableUtils.readString(dataInput);
		wordChiPair = WritableUtils.readString(dataInput);
	}

	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, category);
		WritableUtils.writeString(dataOutput, wordChiPair);
	}

	public int compareTo(CategoryWordChiPair objKeyPair) {
		int result = category.compareTo(objKeyPair.category);
		if (0 == result) {
			result = wordChiPair.split(",")[1].compareTo(objKeyPair.wordChiPair.split(",")[1]);
		}
		return result;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public String getWordChiPair() {
		return wordChiPair;
	}

	public void setWordChiPair(String wordCategoryPair) {
		this.wordChiPair = wordCategoryPair;
	}
}