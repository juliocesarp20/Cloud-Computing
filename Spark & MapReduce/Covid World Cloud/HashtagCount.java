package hashtagcount;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import hashtagcount.TextArrayWritable;
import com.google.gson.Gson;
import com.google.gson.JsonParser;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
//Creates the class that will hold Mapper and Reducer
public class HashtagCount {
    //Creates a Mapper
    public static class TokenizerMapper
    extends Mapper < Object, Text, Text, TextArrayWritable > {
        //Creates a variable to store the key
        private Text word = new Text();
        //@map receives the JSON stored inputs (each line should be a input)
        public void map(Object key, Text value, Context context)
        throws IOException,
        InterruptedException {
            //Creates a regex to find hashtags
            String regexPattern = "(#\\w+)";
            Pattern p = Pattern.compile(regexPattern);
            //Create a HashSet to store unique hashtags
            HashSet < String > hashtags = new HashSet < String > ();
            //For each line, convert the line to JSON and get the tweet text
            Scanner scanner = new Scanner(value.toString());
            while (scanner.hasNextLine()) {
                try {
                    String line = scanner.nextLine();
                    JsonObject jsonObject = new JsonParser().parse(line).getAsJsonObject();
                    String hs = jsonObject.get("text").getAsString();
                    Matcher m = p.matcher(hs);
                    //For each word in the line with a hashtag, add them to a array
                    while (m.find()) {
                        String hashtag = m.group(1);
                        hashtag = hashtag.toUpperCase();
                        hashtags.add(hashtag);
                    }
                } catch (Exception e) {
                }
            }

            scanner.close();
            //If there are hashtags to add
            if (hashtags.size() > 0) {
                ArrayList < String > hashtags2 = new ArrayList < String > ();
                //For each hashtag, find all correlated hashtags and add them to the array
                for (String s: hashtags) {
                    for (String s2: hashtags) {
                        if (!s.equals(s2)) {
                            hashtags2.add(s2);
                        }
                    }
                    //Pass the hashtag array and key to the reducer
                    String[] hashtagsArray = new String[hashtags2.size()];
                    hashtagsArray = hashtags2.toArray(hashtagsArray);
                    TextArrayWritable hashtagsW = new TextArrayWritable(hashtagsArray);
                    word.set(s);
                    context.write(word, hashtagsW);
                    //Clear the hashtag array for the next loop
                    hashtags2.clear();
                }

            }
        }
    }


    //Creates a hashtag reducer
    public static class HashtagArrayReducer
    extends Reducer < Text, TextArrayWritable, Text, TextArrayWritable > {

        //Result stores the hashtags
        private TextArrayWritable result = new TextArrayWritable();

        //@reduce receives the hashtag key and hashtag array
        public void reduce(Text key, Iterable < TextArrayWritable > values, Context ctx)
        throws IOException,
        InterruptedException {
            //Calculate the number of times a hashtag key appears and add all array values to a set
            int val = 0;
            HashSet < String > hashtagsr = new HashSet < String > ();
            for (TextArrayWritable value: values) {
                val++;
                for (Writable w: value.get()) {
                    String str = ((Text) w).toString();
                    hashtagsr.add(str);
                }
            }
            //If the hashtag appears at least 1000 times, add the key count to a array, followed by all the correlated hashtags
            if (val >= 1000) {
                ArrayList < String > results = new ArrayList < String > ();
                results.add(Integer.toString(val));
                results.addAll(hashtagsr);
                String[] hashtagsArrayr = new String[results.size()];
                hashtagsArrayr = results.toArray(hashtagsArrayr);
                TextArrayWritable hashtagsR = new TextArrayWritable(hashtagsArrayr);
                //Write everything, TextArrayWritable overrides the ArrayWritable toString method so it's in csv format
                ctx.write(key, hashtagsR);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "hashtags");
        job.setJarByClass(HashtagCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(HashtagArrayReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TextArrayWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}