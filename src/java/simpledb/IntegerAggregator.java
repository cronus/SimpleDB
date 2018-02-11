package simpledb;

import java.util.*;
/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private int count;
    private int sum;
    private int min;
    private int max;

    private List<Tuple> aggregateTuples;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield         = gbfield;
        this.gbfieldtype     = gbfieldtype;
        this.afield          = afield;
        this.what            = what;
        this.aggregateTuples = new ArrayList<Tuple>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        //System.out.println(tup);
        TupleDesc td = tup.getTupleDesc();
        // No_GROUPING
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
            Type[]   type = {td.getFieldType(gbfield), td.getFieldType(afield)};
            String[] str  = {td.getFieldName(gbfield), td.getFieldName(afield)};
            TupleDesc atd = new TupleDesc(type, str);
            Field gf      = tup.getField(gbfield);
            Field af      = tup.getField(afield);
            // if the aggregator tuples contains the group value
            // merge
            boolean nogv = true;
            Iterator<Tuple> it = aggregateTuples.iterator();
            while(it.hasNext()) {
                Tuple t = it.next();
                if (t.getField(0).equals(gf)) {
                    nogv = false;
                    sum += af.hashCode();
                    count++;
                    if (min > af.hashCode())
                        min = af.hashCode();
                    if (max < af.hashCode())
                        max = af.hashCode();
                    if (what == Aggregator.Op.MIN) {
                        t.setField(1, new IntField(min));
                    }
                    else if (what == Aggregator.Op.MAX) {
                        t.setField(1, new IntField(max));
                    }
                    else if (what == Aggregator.Op.SUM) {
                        t.setField(1, new IntField(sum));
                    }
                    else if (what == Aggregator.Op.AVG) {
                        t.setField(1, new IntField(sum/count));
                    }
                    else if (what == Aggregator.Op.COUNT) {
                        t.setField(1, new IntField(count));
                    }
                    else {
                        System.out.println("Unsupported operator!");
                        throw new IllegalArgumentException();
                    }
                    break;
                }
            }
            // not find the same group value
            if (nogv) {
                Tuple newt = new Tuple(atd);
                newt.setField(0, gf);
                count = 1;
                sum   = af.hashCode();
                min   = af.hashCode();
                max   = af.hashCode();
                if (what == Aggregator.Op.MIN) {
                    newt.setField(1, new IntField(min));
                }
                else if (what == Aggregator.Op.MAX) {
                    newt.setField(1, new IntField(max));
                }
                else if (what == Aggregator.Op.SUM) {
                    newt.setField(1, new IntField(sum));
                }
                else if (what == Aggregator.Op.AVG) {
                    newt.setField(1, new IntField(sum/count));
                }
                else if (what == Aggregator.Op.COUNT) {
                    newt.setField(1, new IntField(count));
                }
                else {
                    System.out.println("Unsupported operator!");
                    throw new IllegalArgumentException();
                }
                aggregateTuples.add(newt);
                //System.out.println(newt);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        //throw new
        //UnsupportedOperationException("please implement me for lab2");
        //System.out.println("int aggre size:"+aggregateTuples.size());
        return new TupleIterator(aggregateTuples.get(0).getTupleDesc(), aggregateTuples);
    }

}
