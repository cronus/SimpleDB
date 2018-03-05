package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator[] children;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;

    private int position;
    private List<Tuple> tuples;
    private Aggregator aggre;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
        this.children      = new OpIterator[1];
        this.children[0]   = child;
        this.afield        = afield;
        this.gfield        = gfield;
        this.aop           = aop;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
	//return -1;
        if (gfield != Aggregator.NO_GROUPING)
            return gfield;
        else
            return Aggregator.NO_GROUPING;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
	//return null;
        return children[0].getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
	//return -1;
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
	//return null;
        return children[0].getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
	//return null;
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	    return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
        super.open();
        children[0].open();
        position = 0;
        tuples       = new ArrayList<Tuple>();
        TupleDesc td = children[0].getTupleDesc();
        if (gfield != Aggregator.NO_GROUPING) {
            Type gt      = td.getFieldType(gfield);
            if (td.getFieldType(afield) == Type.INT_TYPE) {
                aggre = new IntegerAggregator(gfield, gt, afield, aop);
            }
            else if (td.getFieldType(afield) == Type.STRING_TYPE) {
                aggre = new StringAggregator(gfield, gt, afield, aop);
            }
        }
        else {
            if (td.getFieldType(afield) == Type.INT_TYPE) {
                aggre = new IntegerAggregator(gfield, null, afield, aop);
            }
            else if (td.getFieldType(afield) == Type.STRING_TYPE) {
                aggre = new StringAggregator(gfield, null, afield, aop);
            }
        }
        while (children[0].hasNext()) {
            Tuple t = children[0].next();
            aggre.mergeTupleIntoGroup(t);
        }
        OpIterator oit = aggre.iterator();
        oit.open();
        while (oit.hasNext()) {
            Tuple t = oit.next();
            tuples.add(t);
        }
        //System.out.println("size:"+tuples.size());
        oit.close();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
	//return null;
        if (position < tuples.size()) {
            return tuples.get(position++);
        }
        else
            return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
        children[0].rewind();
        position = 0;
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
	//return null;
        if (gfield == Aggregator.NO_GROUPING) {
            Type[] t   = {children[0].getTupleDesc().getFieldType(afield)};
            String[] s = {children[0].getTupleDesc().getFieldName(afield)};
            return new TupleDesc(t, s);
        }
        else {
            Type[] t = {children[0].getTupleDesc().getFieldType(gfield), 
                        children[0].getTupleDesc().getFieldType(afield)};
            String[] s = {children[0].getTupleDesc().getFieldName(gfield), 
                        children[0].getTupleDesc().getFieldName(afield)};
            return new TupleDesc(t, s);
        }
    }

    public void close() {
	// some code goes here
        super.close();
        children[0].close();
        tuples = null;
    }

    @Override
    public OpIterator[] getChildren() {
	// some code goes here
	//return null;
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
	// some code goes here
        this.children = children;
    }
    
}
