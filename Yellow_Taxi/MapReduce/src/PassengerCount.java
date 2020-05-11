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


public class PassengerCount extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(PassengerCount.class);
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new PassengerCount(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "taxi passengers counter");
        job.setJarByClass(this.getClass());
        // Use TextInputFormat, the default unless job.setInputFormatClass is used
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(PassengerCountMapper.class);
        job.setCombinerClass(PassengerCountReducer.class);
        job.setReducerClass(PassengerCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
    public static class PassengerCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private IntWritable passengercount = new IntWritable();
        private Text montharea = new Text();

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            try {
                if (offset.get() == 0)
                    return;
                else {
                    String line = lineText.toString();
                    String temporarymonth = new String();
                    int i = 0;
                    for (String word : line
                            .split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")) {
                        if (i == 1) { // first columns corresponds to pick up date
                            temporarymonth = word.substring(word.lastIndexOf('/') - 2,
                                    word.lastIndexOf('/')); // uses 2 digit that stays between backslashes
                        }
                        if (i == 3) { // third column correspond to no. of passengers
                            passengercount.set(Integer.parseInt(word));
                        }
                        if (i == 7) { // 7th column stored pick up location number
                            montharea.set(temporarymonth + " : " + Integer.parseInt(word));
                        }
                        if (i == 9) {
                          if (word.equals("2")) { // writing out only cash payments
                             context.write(montharea, passengercount);
                           }
                        }
                        i++;
                      }
                    }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class PassengerCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        int sum;
        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            sum = 0;

            Text sumText = new Text("Passengers number that payed with cash for " + key
                    + " month is: ");

            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(sumText, result);
        }
    }
}
