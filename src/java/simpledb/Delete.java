package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private OpIterator[] children;

    private int position;
    private int count;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.tid         = t;
        this.children    = new OpIterator[1];
        this.children[0] = child;

        this.position    = 0;
        this.count       = 0;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        //return null;
        Type[] t     = {Type.INT_TYPE};
        String[] str = {"COUNT"};
        TupleDesc td = new TupleDesc(t, str);
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        children[0].open();
        position = 0;
        count = 0;

        BufferPool bp = Database.getBufferPool();

        try {
            while (children[0].hasNext()) {
                Tuple t = children[0].next();
                bp.deleteTuple(tid, t);
                count++;
            }
        } catch (IOException e) {
        }
    }

    public void close() {
        // some code goes here
        super.close();
        children[0].close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        children[0].rewind();
        position = 0;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        //return null;
        if (position == 0) {
            TupleDesc td = getTupleDesc();
            Tuple tuple  = new Tuple(td);
            tuple.setField(0, new IntField(count));
            position++;
            return tuple;
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
