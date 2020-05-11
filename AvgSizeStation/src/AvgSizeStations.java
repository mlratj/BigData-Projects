import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;


public class AvgSizeStations extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(AvgSizeStations.class);
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new AvgSizeStations(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "Average Size of each Station");
        job.setJarByClass(this.getClass());
        // Use TextInputFormat, the default unless job.setInputFormatClass is used
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(AvgSizeStationMapper.class);
        job.setReducerClass(AvgSizeStationsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
    public static class AvgSizeStationMapper extends Mapper<LongWritable, Text, Text, IntWritable> { // co czyta i co zwraca

        private Text year = new Text();
        private IntWritable size = new IntWritable();

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            try {
                if (offset.get() == 0)
                    return;
                else {
                    String line = lineText.toString();
                    int i = 0;
                    for (String word : line
                            .split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")) {
                        if (i == 4) { // checks 4th columns
                            year.set(word.substring(word.lastIndexOf('/') + 1, // takes 2 digit as a month
                                    word.lastIndexOf('/') + 5)); // takes 4 digits as a year
                        }
                        if (i == 5) { // checks 5th columns
                            size.set(Integer.parseInt(word));
                        }
                        i++;
                    }
                    context.write(year, size);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            }
        }

    public static class AvgSizeStationsReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        Float average;
        Float count;
        int sum;
        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            average = 0f;
            count = 0f;
            sum = 0;

            Text sumText = new Text("average size of station for " + key
                    + " year is: ");

            for (IntWritable val : values) {
                sum += val.get();
                count += 1;
            }
            average = sum / count;
            result.set(average);
            context.write(sumText, result);
        }
        }
    }
