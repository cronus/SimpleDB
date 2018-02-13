package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {


    private File f;
    private TupleDesc td;
    private List<Tuple> tuples;
    private ArrayList<Page> pages;
    private BufferPool bp = Database.getBufferPool();
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f      = f;
        this.td     = td;
        this.tuples = new ArrayList<Tuple>();
        this.pages  = new ArrayList<Page>();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        //return null;
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return (int)(byte)f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return td;
    }

    // see DbFile.java for javadocs
    // @throws IllegalArgumentException if the page does not exist in this file.
    public Page readPage(PageId pid) {
        // some code goes here
        //return null;
        // check whether the page exists in this file
        if (pid.getPageNumber() >= numPages())
           throw new IllegalArgumentException();
        HeapPageId hpId = new HeapPageId(pid.getTableId(), pid.getPageNumber());
        HeapPage pg;
        // read the data from File f to byte[] buf
        byte[] buf = new byte[(int) f.length()];
        //System.out.println(f.length());
        int offset = pid.getPageNumber() * Database.getBufferPool().getPageSize();
        int count  = 0;
        try {
            InputStream is  = new FileInputStream(f);
            is.skip(offset);
            while (offset < buf.length
                   && (count = is.read(buf, 0, buf.length - offset)) >= 0) {
                offset += count;
            }
            is.close();
            pg     = new HeapPage(hpId, buf);
            //if (count <= 4096) {
            //    System.out.println("not first page of file");
            //    Iterator<Tuple> it = pg.iterator();
            //    while(it.hasNext()) {
            //        System.out.println("ddd:"+it.next());
            //    }
            //}
        } catch (IOException e) {
            return null;
        }
        return pg;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        FileOutputStream os = new FileOutputStream(f, true);
        byte[] pageData = page.getPageData();
        os.write(pageData);
        os.close();
    }

    /**
     * Returns the number of pags in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        //return 0;
        return (int) Math.ceil(f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        //return null;
        // not necessary for lab1
        int n              = numPages();
        int tableid        = getId();
        boolean insertDone = false;

        pages.clear();
        for (int i = 0; i < n; i++) {
            HeapPageId hpId = new HeapPageId(tableid, i);
            HeapPage hp     = (HeapPage) bp.getPage(tid, hpId, Permissions.READ_WRITE);
            if (hp.getNumEmptySlots() != 0) {
                hp.insertTuple(t);
                insertDone = true;
                break;
            }
        }

        //if all pages are full, append a new page
        if (!insertDone) {
            HeapPageId nhpId = new HeapPageId(tableid, n);
            HeapPage nhp     = new HeapPage(nhpId, HeapPage.createEmptyPageData());
            writePage(nhp);
            nhp              = (HeapPage) bp.getPage(tid, nhpId, Permissions.READ_WRITE);
            nhp.insertTuple(t);
        }

        for (int i = 0; i < n; i++) {
            HeapPageId hpId2 = new HeapPageId(tableid, i);
            HeapPage hp2 = (HeapPage) bp.getPage(tid, hpId2, Permissions.READ_WRITE);
            pages.add(hp2);
        }
        return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        //return null;
        // not necessary for lab1
        pages.clear();
        HeapPageId hpId = (HeapPageId) t.getRecordId().getPageId();
        HeapPage hp = (HeapPage) bp.getPage(tid, hpId, Permissions.READ_WRITE);
        hp.deleteTuple(t);

        int n       = numPages();
        int tableid = getId();
        for (int i = 0; i < n; i++) {
            HeapPageId hpId2 = new HeapPageId(tableid, i);
            HeapPage hp2 = (HeapPage) bp.getPage(tid, hpId2, Permissions.READ_WRITE);
            pages.add(hp2);
        }
        return pages;
    }

    private class HeapFileIterator extends AbstractDbFileIterator {
        private int position = 0;
        private List<Tuple> t;
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            if (t == null)
                return null;
            else if (position < t.size())
                return t.get(position);
            else
                return null;
        }

        public Tuple next() throws DbException, TransactionAbortedException, 
                NoSuchElementException {
            Tuple t = super.next();
            position++;
            return t;
        }

        public void rewind() throws DbException, TransactionAbortedException {
            position = 0;
        }
        
        // open the iterator
        public void open() throws DbException, TransactionAbortedException {
            t = tuples;
        }
        // close the iterator
        public void close() {
            t = null;
            super.close();
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        //return null;
        int n       = numPages();
        //System.out.println(n);
        int tableid = getId();
        tuples.clear();
        try {
            for (int i = 0; i < n; i++) {
                HeapPageId hpId = new HeapPageId(tableid, i);
                HeapPage hp = (HeapPage) bp.getPage(tid, hpId, Permissions.READ_ONLY);
                Iterator<Tuple> it = hp.iterator();
                while(it.hasNext()) {
                    Tuple t = it.next();
                    //System.out.println("heapfile:"+i+"--"+ t);
                    tuples.add(t);
                }
            }
        } catch (TransactionAbortedException e) {
            return null;
        } catch (DbException e) {
            return null;
        }
        return new HeapFileIterator();
    }

}

