import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BaseballAnalytics {

	public static class BaseballAnalyticsMapper extends Mapper<Object, Text, Text, LongWritable> {

		private Text team = new Text();
		private LongWritable games = new LongWritable();

		public void map(Object offset, Text csv, Context context) throws IOException, InterruptedException {

			if(((LongWritable)offset).get() == 0) {
				return;
			}
			
			String[] fields = csv.toString().split(",");
			String teamID = fields[3];
			int gamesCount = Integer.parseInt(fields[6]);

			team.set(teamID);
			games.set(gamesCount);

			context.write(team, games);
		}
	}

	public static class TeamsReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		private LongWritable result = new LongWritable();

		public void reduce(Text team, Iterable<LongWritable> games, Context context) throws IOException, InterruptedException {

			long sum = 0;
			
			for (LongWritable val : games) {

				sum += val.get();
;	
			}

			result.set(sum);

			context.write(team,result);
		}
	}


	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "baseball analytics");
		job.setJarByClass(BaseballAnalytics.class);
		job.setMapperClass(BaseballAnalyticsMapper.class);
		job.setCombinerClass(TeamsReducer.class);
		job.setReducerClass(TeamsReducer.class);
		job.setOutputValueClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		System.out.println("Input format class: " + job.getInputFormatClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}