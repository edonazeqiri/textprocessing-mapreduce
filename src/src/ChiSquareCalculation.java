

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

public class ChiSquareCalculation {

	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private BufferedReader br;
		private Configuration conf;
		private final JSONParser parser = new JSONParser();
		private Set<String> skipWords = new HashSet<String>();
		private static final IntWritable one = new IntWritable(1);

		private enum Counters {
			RowsNo
		}

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			if (conf.getBoolean("skip.words", false)) {
				URI[] cachedFiles = Job.getInstance(conf).getCacheFiles();
				for (URI stopWordsURI : cachedFiles) {
					Path stopWordsPath = new Path(stopWordsURI.getPath());
					String fileName = stopWordsPath.getName().toString();
					parseFile(fileName);
				}
			}
		}

		private void parseFile(String fileName) {
			try {
				br = new BufferedReader(new FileReader(fileName));
				String pattern = null;
				while ((pattern = br.readLine()) != null) {
				System.out.println(pattern);
					skipWords.add(pattern);
				}

			} catch (IOException ioe) {
				System.err.println("Caught exception while parsing the cached file");
			}
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			JSONObject jsonObj;
			try {
				jsonObj = (JSONObject) parser.parse(value.toString());
			} catch (org.json.simple.parser.ParseException e) {
				return;
			}
			context.getCounter(Counters.RowsNo).increment(1);
			String category = jsonObj.get("category").toString().trim();
			String reviewText = jsonObj.get("reviewText").toString().toLowerCase();

			ArrayList<String> uniqueWords = new ArrayList<String>();

			reviewText = reviewText.replaceAll("\\d+", " "); // replace all digits with an empty space

			String delimiters = " \t.!?,;:()[]{}-\"`~#&*%$\\/";

			StringTokenizer itr = new StringTokenizer(reviewText, delimiters);
			while (itr.hasMoreTokens()) {
				String word = itr.nextToken().trim();
				// only include unique words per review, which have more than
				// one character and aren't skipwords
				if (uniqueWords.contains(word) || word.length() <= 1 || skipWords.contains(word)) {
					continue;
				}
				uniqueWords.add(word);
				context.write(new Text(category + "," + word), one);
				context.write(new Text("word-" + word), one);
			}
			context.write(new Text("category-" + category), one);

		}
	}

	public static class TokenizerSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static class ChiSquareTopPairsMapper extends Mapper<LongWritable, Text, CategoryWordChiPair, NullWritable> {

		double RowsNumber = 0;
		private Configuration conf;
		private BufferedReader br;
		private static Map<String, Integer> words = new HashMap<String, Integer>();
		private static Map<String, Integer> categories = new HashMap<String, Integer>();

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			RowsNumber = conf.getDouble("RowsNo", 0);
			URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
			Path patternsPath = new Path(patternsURIs[0].getPath());
			String patternsFileName = patternsPath.getName().toString();
			parseFile(patternsFileName);
		}

		// terms that contain word count and category count will be put on
		// hashmaps, words and categories hashmaps
		private void parseFile(String fileName) {
			try {
				br = new BufferedReader(new FileReader(fileName));
				String pattern = null;
				while ((pattern = br.readLine()) != null) {
					String key = pattern.split("\t")[0].trim();
					Integer value = Integer.parseInt(pattern.split("\t")[1].trim());
					if (pattern.contains("category-")) {
						categories.put(key.split("-")[1], value);
					} else if (pattern.contains("word-")) {
						words.put(key.split("-")[1], value);
					}
				}
			} catch (IOException ioe) {
				System.err.println("Exception parsing the file");
			}
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// include only values that contain <category, word> pair
			if (!(value.toString().contains("word-") || value.toString().contains("category-"))) {
				String[] values = value.toString().split("\t");
				String keyPairs = values[0].trim();
				double categoryNo = categories.get(keyPairs.split(",")[0].trim());
				double wordNo = words.get(keyPairs.split(",")[1].trim());
				double N = RowsNumber;
				double A = Double.parseDouble(values[1].trim());
				double B = wordNo - A;
				double C = categoryNo - A;
				double D = N - A - B - C;
				double chi_square = N*Math.pow(((A * D) - (B * C)), 2) / ((A + B) * (A + C) * (B + D) * (C + D));

				context.write(new CategoryWordChiPair(keyPairs.split(",")[0].trim(),
						keyPairs.split(",")[1].trim() + "," + chi_square), NullWritable.get());
			}
		}
	}

	public static class ChiSquareTopPairsReducer
			extends Reducer<CategoryWordChiPair, NullWritable, Text, NullWritable> {

		int TopValues;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			TopValues = context.getConfiguration().getInt("TopValues", 0);
		}

		public void reduce(CategoryWordChiPair key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			String wordChiPairs = "";
			int count = 0;
			
			for (@SuppressWarnings("unused") NullWritable val : values) {
				if (count < TopValues) {
					wordChiPairs += key.getWordChiPair().split(",")[0] + ":" + key.getWordChiPair().split(",")[1] + " ";
				}
				count++;
			}
			context.write(new Text(key.getCategory() + " " + wordChiPairs.trim()), NullWritable.get());
		}
	}

	public static class AllTermsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] terms = value.toString().split(" ");
			for (int i = 1; i < terms.length; i++)
				context.write(new Text(terms[i].split(":")[0]), new IntWritable(1));
		}
	}

	public static class AllTermsReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

		String allTerms = "";

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			allTerms += key.toString() + " ";
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new Text(allTerms.toString()), NullWritable.get());
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);
		String[] argsList = optionsParser.getRemainingArgs();

		FileSystem fs = FileSystem.get(conf);
		int topValues = 0;
		double rowsNo = 0;
		String filterCountPath = "/user/e11946217/tokenization";
		List<String> remainingArgs = new ArrayList<String>();
		{
			Job job1 = Job.getInstance(conf, "STEP 1: Filtering review text; counting words, categories and <word, category> pairs");
			job1.setJarByClass(ChiSquareCalculation.class);
			job1.setMapperClass(TokenizerMapper.class);
			job1.setCombinerClass(TokenizerSumReducer.class);
			job1.setReducerClass(TokenizerSumReducer.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntWritable.class);

			for (int i = 0; i < argsList.length; ++i) {
				if ("-top".equals(argsList[i])) {
					topValues = Integer.parseInt(argsList[++i].toString());
				} else if ("-skip".equals(argsList[i])) {
					job1.addCacheFile(new Path(argsList[++i]).toUri());
					job1.getConfiguration().setBoolean("skip.words", true);
				} else {
					remainingArgs.add(argsList[i]);
				}
			}

			fs.delete(new Path(filterCountPath), true);
			fs.delete(new Path(remainingArgs.get(1)), true);

			FileInputFormat.addInputPath(job1, new Path(remainingArgs.get(0)));
			FileOutputFormat.setOutputPath(job1, new Path(filterCountPath));

			System.out.println(job1.getJobName());
			job1.waitForCompletion(true);
			rowsNo = job1.getCounters().findCounter(TokenizerMapper.Counters.RowsNo).getValue();
		}

		{
			Job job2 = Job.getInstance(conf, "STEP 2: Calculating chi-square values, sorting and getting top N values");
			job2.setJarByClass(ChiSquareCalculation.class);
			job2.setMapperClass(ChiSquareTopPairsMapper.class);
			job2.setReducerClass(ChiSquareTopPairsReducer.class);
			job2.setPartitionerClass(PartitionerSecondarySort.class);
			job2.setSortComparatorClass(KeySortComparator.class);
			job2.setGroupingComparatorClass(GroupComparator.class);
			job2.setMapOutputKeyClass(CategoryWordChiPair.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(NullWritable.class);
			job2.getConfiguration().setInt("TopValues", topValues);
			job2.getConfiguration().setDouble("RowsNo", rowsNo);
			job2.addCacheFile(new Path(filterCountPath + "/part-r-00000").toUri());

			FileInputFormat.addInputPath(job2, new Path(filterCountPath));
			FileOutputFormat.setOutputPath(job2, new Path(remainingArgs.get(1)));

			System.out.println(job2.getJobName());
			if (job2.waitForCompletion(true)) {
				if (fs.exists(new Path(filterCountPath))) {
					fs.delete(new Path(filterCountPath), true);
				}
			}
		}

		{
			Job job3 = Job.getInstance(conf, "STEP 3: Creating a line of space-separated and ordered alphabetically terms");
			String allTermsFile = remainingArgs.get(1) + "/all_terms";
			job3.setJarByClass(ChiSquareCalculation.class);
			job3.setMapperClass(AllTermsMapper.class);
			job3.setReducerClass(AllTermsReducer.class);
			job3.setMapOutputValueClass(IntWritable.class);
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(NullWritable.class);

			FileInputFormat.addInputPath(job3, new Path(remainingArgs.get(1)));
			FileOutputFormat.setOutputPath(job3, new Path(allTermsFile));

			System.out.println(job3.getJobName());
			System.exit(job3.waitForCompletion(true) ? 0 : 1);
		}
	}

}