import au.com.bytecode.opencsv.CSVReader;

import java.io.*;

import com.mongodb.Mongo;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;

/**
 * <tt>LoadFootballData</tt>
 *
 * @author harris
 */
public class LoadFootballData
  {
  public static final String DATA_PATH = "data/ProFootball2007Season.csv";
  private static CSVReader csvReader;

  public static void main(String[] args) throws Exception
    {
    InputStream stream = new FileInputStream(DATA_PATH);
    Reader reader = new BufferedReader(new InputStreamReader(stream));
    csvReader = new CSVReader(reader);
    String[] header = csvReader.readNext();
    processRows(header);
    }

  private static void processRows(String[] header)
    throws IOException
    {
    long start = System.currentTimeMillis();
    System.out.println("beginning to load data");
    int chunksize = 500;
    int currentIndex = 0;
    String[] nextLine;
    Mongo m = new Mongo();
    DB db = m.getDB("AggregationExampleDB");
    DBCollection footballCollection = db.getCollection("ProFootball2007Season");
    footballCollection.drop();

    while ((nextLine = csvReader.readNext()) != null)
      {
      BasicDBObject document = new BasicDBObject();

      for (int i = 0; i < header.length; i++)
        {
        String value = null;

        try
          {
          value = nextLine[i];
          }
        catch (Exception e)
          {
          value = null;
          }

        if (value == null || value.length() == 0)
          continue;

        try
          {
          double d = Double.parseDouble(value.toString());
          document.put(header[i].trim(), d);
          }
        catch (NumberFormatException exception)
          {
          document.put(header[i].trim(), value.trim());
          }
        }

      footballCollection.insert(document);
      currentIndex++;

      if (currentIndex % chunksize == 0)
        {
        long time = System.currentTimeMillis() - start;
        System.out.println("loaded chunk of data, total: " + currentIndex + ", time in sec: " + time / 1000);
        }
      }

    long time = System.currentTimeMillis() - start;
    System.out.println("total time in sec: " + time / 1000 + ", total rows: " + currentIndex);
    }
  }
