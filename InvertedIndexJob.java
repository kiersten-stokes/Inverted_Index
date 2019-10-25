import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;
import java.lang.StringBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndexJob {
	
	public static final String specialChars = "[^a-zA-Z]+";

	public static class InvIndexMapper
		extends Mapper<Object, Text, Text, Text>{

		private Text word = new Text();
		private Text docID = new Text();

		public void map(Object key, Text value, Context context) 
						throws IOException, InterruptedException {
							
			/* Split text into docID and text */
			String[] lines = value.toString().split("\t", 2);
			String docid = lines[0];
			docID.set(docid);
			
			/* Replace all the occurrences of special characters 
			 * and numerals with a space character */
			String alphaOnly = lines[1].replaceAll(specialChars, " ");
			
			/* Using the default delimiter set, 
			 * iterate through text to get words */
			StringTokenizer itr = new StringTokenizer(alphaOnly);
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken().toLowerCase());
				context.write(word, docID);
			}
		}
	}

	public static class InvIndexReducer
		extends Reducer<Text,Text,Text,Text> {

		private Text postingsList = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) 
							throws IOException, InterruptedException {
								
			/* Initiate and fill hashmap of document-term frequencies */
			HashMap<String, Integer> tfMap = new HashMap<String, Integer>();
			for (Text doc : values) {
				String docID = doc.toString();
				int tf = tfMap.containsKey(docID) ? tfMap.get(docID) + 1 : 1;
				tfMap.put(docID, tf);
			}

			/* Iterate through hashmap and add to a psuedo-postings list */
			StringBuilder list = new StringBuilder();
			for (Map.Entry <String, Integer> entry: tfMap.entrySet()) {
				String value = entry.getKey();
				list.append(value + ":" + entry.getValue() + " ");
			}

			postingsList.set(list.toString());
			context.write(key, postingsList);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t\t");
		Job job = Job.getInstance(conf, "inverted index");
		job.setJarByClass(InvertedIndexJob.class);
		job.setMapperClass(InvIndexMapper.class);
		job.setReducerClass(InvIndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}