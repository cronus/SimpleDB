package simpledb;

import java.util.*;
/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private int count;
    private List<Tuple> aggregateTuples;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield     = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield      = afield;
        this.what        = what;
        this.aggregateTuples = new ArrayList<Tuple>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        TupleDesc td = tup.getTupleDesc();
        if (gbfield == -1) {
            Type[] type    = {td.getFieldType(afield)};
            String[] str   = {td.getFieldName(afield)};
            TupleDesc atd  = new TupleDesc(type, str);
            Tuple newngt   = new Tuple(atd);
            Field af       = tup.getField(afield);
            newngt.setField(0, af);
            aggregateTuples.add(newngt);
        } 
        else {
            Type[]   type = {td.getFieldType(gbfield), Type.INT_TYPE};
            String[] str  = {td.getFieldName(gbfield), "COUNT"};
            TupleDesc atd = new TupleDesc(type, str);
            Field gf      = tup.getField(gbfield);
            //Field af      = tup.getField(afield);
            boolean nogv = true;
            Iterator<Tuple> it = aggregateTuples.iterator();
            while(it.hasNext()) {
                Tuple t = it.next();
                if (t.getField(0).hashCode() == gf.hashCode()) {
                    nogv = false;
                    count++;
                    if (what == Aggregator.Op.COUNT) {
                        t.setField(1, new IntField(count));
                    }
                    else {
                        System.out.println("Unsupported operator!");
                        throw new IllegalArgumentException();
                    }
                    break;
                }
            }
            //System.out.println(nogv);
            if (nogv) {
                Tuple newt = new Tuple(atd);
                newt.setField(0, gf);
                count = 1;
                if (what == Aggregator.Op.COUNT) {
                    newt.setField(1, new IntField(count));
                }
                else {
                    System.out.println("Unsupported operator!");
                    throw new IllegalArgumentException();
                }
                //System.out.println(newt);
                aggregateTuples.add(newt);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        //throw new UnsupportedOperationException("please implement me for lab2");
        return new TupleIterator(aggregateTuples.get(0).getTupleDesc(), aggregateTuples);
    }

}
