package hashtagcount;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
//Extends ArrayWritable to send array of strings
public class TextArrayWritable extends ArrayWritable {
    public TextArrayWritable() {
        super(Text.class);
    }
    //Convert the text to a array of strings
    public TextArrayWritable(String[] values) {
        super(Text.class);
        Text[] convertedValues = new Text[values.length];
        for (int i = 0; i < values.length; i++) {
            convertedValues[i] = new Text(values[i]);
        }
        set(convertedValues);
    }
    //Helper function to format strings
    private String getFormated(String[] strings) {
        StringBuilder sbr = new StringBuilder();
        sbr.append(strings[0]).append(",");
        for (int i = 1; i < strings.length - 1; i++) {
            sbr.append(strings[i]).append(" ");
        }
        sbr.append(strings[strings.length - 1]);
        return sbr.toString();
    }
    //Return formated strings in csv readable format
    @Override
    public String toString() {
        String[] result = super.toStrings();
        return getFormated(result);
    }
}