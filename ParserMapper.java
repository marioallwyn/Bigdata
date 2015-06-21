@@ -0,0 +1,62 @@
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

/**
 *
 * @author Sonam
 */
public class ParserMapper extends Mapper<LongWritable, Text, NullWritable, Text> {



    @Override
    public void map(LongWritable key, Text value1,Context context)

throws IOException, InterruptedException {

                String xmlString = value1.toString();
              
             SAXBuilder builder = new SAXBuilder();
            Reader in = new StringReader(xmlString);
    String value="";
        try {
            
            Document doc = builder.build(in);
            Element root = doc.getRootElement();
            
            String tag1 =root.getChild("title").getTextTrim() ;
             
            String tag2 =root.getChild("revision").getChild("text").getTextTrim();
             value= tag1+ ","+tag2;
             
            Pattern p = Pattern.compile("\\[\\[([^\\[\\]|]*)[^\\[\\]]*\\]\\]");
     		Matcher m = p.matcher(tag2);
     		
     		while (m.find()) {
     			value= tag1+ ","+m.group(1);
     			context.write(NullWritable.get(), new Text(value));
     		}
             
        } catch (JDOMException ex) {
            Logger.getLogger(ParserMapper.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(ParserMapper.class.getName()).log(Level.SEVERE, null, ex);
        }
    
    }

}
