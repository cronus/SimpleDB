package simpledb;

import java.io.*;
import java.util.*;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private OpIterator[] children;
    private int tableId;

    private int position;
    private List<Tuple> tuples;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.tid         = t;
        this.children    = new OpIterator[1];
        this.children[0] = child;
        this.tableId     = tableId;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        //return null;
        return children[0].getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        children[0].open();
        position = 0;

        BufferPool bp = Database.getBufferPool();
        tuples   = new ArrayList<Tuple>();

        try {
            while (children[0].hasNext()) {
                Tuple t = children[0].next();
                bp.insertTuple(tid, tableId, t);
            }
        } catch (IOException e) {
        }
    }

    public void close() {
        // some code goes here
        super.close();
        children[0].close();
        tuples = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        children[0].rewind();
        position = 0;

        //insert child into table of tableId
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
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

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        //return null;
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        children = children;
    }
}
