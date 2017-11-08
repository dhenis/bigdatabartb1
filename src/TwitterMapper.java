// deni setiawan msc Software Engineering

import java.lang.Math;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;

public class TwitterMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final IntWritable one = new IntWritable(1);

    private Text data = new Text();

    //1469453965000;757570957502394369;Over 30 million women footballers in the world. Most of us would trade places with this lot for #Rio2016  https://t.co/Mu5miVJAWx;<a href="http://twitter.com/download/iphone" rel="nofollow">Twitter for iPhone</a>

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //String clean =  value.toString().replaceAll("; ", ""); // cleaning string(data) from "; " ==> because semicolon is used for split

        try {

          String[] itr = value.toString().split(";"); // exploed and parsed to array and data type is string fix

          if(itr.length >= 4){ // manage if failed to split

            long epoch = Long.parseLong(itr[0]); // parse epoch time to Long

            Date date = new Date(epoch); // create format date form epoch

            SimpleDateFormat time = new SimpleDateFormat("HH"); // set time format for hour only

            time.setTimeZone(TimeZone.getTimeZone("GMT-3")); // I assume that GMT- is rio de janeiro time summer time

            String timeFix = time.format(date); // convert time to string datattoe

            data.set(timeFix); // Set time a as key

            context.write(data, one); // pass (key , value)

            }

        }
        catch (NumberFormatException e) {
              System.err.println("NumberFormatException: " + e.getMessage());

        }    //end catch
    } //end of function
}
