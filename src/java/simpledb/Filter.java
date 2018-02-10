package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private Predicate p;
    private OpIterator[] children;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        this.p           = p;
        this.children    = new OpIterator[1];
        this.children[0] = child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return p;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        //return null;
        return children[0].getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        children[0].open();
    }

    public void close() {
        // some code goes here
        children[0].close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        children[0].rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        //return null;
        while (children[0].hasNext()) {
            Tuple t = children[0].next();
            if (p.filter(t))
                return t;
        }
        throw new NoSuchElementException();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.children = children;
    }

}
