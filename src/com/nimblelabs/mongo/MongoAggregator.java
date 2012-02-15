package com.nimblelabs.mongo;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;

import java.util.List;
import java.util.Comparator;
import java.util.Collections;

/**
 * <tt>MongoAggregator</tt>
 *
 * @author harris
 */
public class MongoAggregator
  {
  private DBCollection collection;

  public MongoAggregator(DBCollection collection)
    {
    this.collection = collection;
    }

  // ********** COUNT METHODS *************************************************

  public List count( String groupByField )
    {
    DBObject columnField = new BasicDBObject(groupByField, true);
    DBObject cond = new BasicDBObject();
    DBObject initial = new BasicDBObject("count", 0);
    String reduce = "function(obj,prev) { prev.count += 1 }";
    String finalize = "";
    DBObject results = collection.group(columnField, cond, initial, reduce, finalize);
    return (List)results;
    }

  public List count( String groupByField, boolean sortDescending )
    {
    List list = count( groupByField );
    sort( "count", sortDescending, list );
    return list;
    }

  public List count( String groupByField, boolean sortDescending, int limit )
    {
    List list = count( groupByField, sortDescending );
    return limitList( list, limit );
    }

  // ********** SUM METHODS ***************************************************

  public List sum( String groupByField, String sumField )
    {
    return sum( groupByField, sumField, new BasicDBObject() );
    }

  public List sum( String groupByField, String sumField, boolean sortDescending )
    {
    return sum( groupByField, sumField, new BasicDBObject(), sortDescending );
    }

  public List sum( String groupByField, String sumField, boolean sortDescending, int limit )
    {
    return sum( groupByField, sumField, new BasicDBObject(), sortDescending, limit );
    }

  public List sum( String groupByField, String sumField, DBObject condition )
    {
    return sum( new BasicDBObject(groupByField, true), sumField, condition );
    }

  public List sum( DBObject groupByMap, String sumField, DBObject condition )
    {
    DBObject initial = new BasicDBObject("sum", 0);
    String reduce = "function(obj,prev) { if( isNaN(obj." + sumField + ") ) return; prev.sum += obj." + sumField + " }";
    String finalize = "";
    DBObject results = collection.group(groupByMap, condition, initial, reduce, finalize);
    return (List)results;
    }

  public List sum( String groupByField, String sumField, DBObject condition, boolean sortDescending )
    {
    List list = sum( groupByField, sumField, condition );
    sort( "sum", sortDescending, list );
    return list;
    }

  public List sum( String groupByField, String sumField, DBObject condition, boolean sortDescending, int limit )
    {
    List list = sum( groupByField, sumField, condition, sortDescending );
    return limitList( list, limit );
    }

  public List sum( DBObject groupByMap, String sumField, DBObject condition, boolean sortDescending, int limit )
    {
    List list = sum( groupByMap, sumField, condition );
    sort( "sum", sortDescending, list );
    return limitList( list, limit );
    }

  // ********** AVG METHODS ***************************************************

  public List avg( String groupByField, String avgField )
    {
    return avg( groupByField, avgField, new BasicDBObject() );
    }

  public List avg( String groupByField, String avgField, DBObject condition )
    {
    DBObject columnField = new BasicDBObject(groupByField, true);
    DBObject initial = new BasicDBObject("sum", 0);
    initial.put("count", 0);
    String reduce = "function(obj,prev) { if( isNaN(obj." + avgField + ") ) return; prev.sum += obj." + avgField + "; prev.count += 1 }";
    String finalize = "function(obj) { obj.avg = obj.sum/obj.count }";
    DBObject results = collection.group(columnField, condition, initial, reduce, finalize);
    return (List)results;
    }

  // ********** EXPERIMENTAL METHODS ******************************************

  public List divideTwoSums( String groupByField, String sumField1, String sumField2, DBObject condition, String resultLabel, boolean sortDescending, int limit )
    {
    DBObject columnField = new BasicDBObject(groupByField, true);
    DBObject initial = new BasicDBObject("sum1", 0);
    initial.put("sum2", 0);
    String sum1Stmt = "if( !isNaN(obj." + sumField1 + ") ) prev.sum1 += obj." + sumField1 +  "; ";
    String sum2Stmt = "if( !isNaN(obj." + sumField2 + ") ) prev.sum2 += obj." + sumField2 +  "; ";
    String reduce = "function(obj,prev) { " + sum1Stmt + sum2Stmt + " }";
    String finalize = "function(obj) { obj." + resultLabel + " = obj.sum1/obj.sum2 }";
    DBObject results = collection.group(columnField, condition, initial, reduce, finalize);

    List list = (List)results;
    Comparator comparator = new FieldComparator( resultLabel, sortDescending );
    Collections.sort( list, comparator );
    return limitList( list, limit );
    }

  // ********** PRIVATE HELPER METHODS ****************************************

  private List limitList( List list, int limit )
    {
    if( list.size() > limit )
      return list.subList( 0, limit );

    return list;
    }

  private void sort( String sortField, boolean sortDescending, List list )
    {
    Comparator comparator = new FieldComparator( sortField, sortDescending );
    Collections.sort( list, comparator );
    }
  }
