package simpledb;

import java.io.*;
import java.util.*;

import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private ConcurrentHashMap<Integer, Page> buffers = null;

    private ConcurrentHashMap<Integer, Lock> locks = null;

    private int pages;

    private Object xLock = new Object();
    private Object sLock = new Object();

    public static class Lock {
        public enum LockType {NO_LOCK, X, S;}

        LockType l;
        
        private TransactionId xltid;
        private ArrayList<TransactionId> sltids;
        

        public Lock (TransactionId tid, LockType l) {
            this.l      = l;
            this.sltids = new ArrayList<TransactionId>();
            if (l == LockType.S) {
                //System.out.println("[class Lock] constructor S lock");
                sltids.add(tid);
                this.xltid = null;
            }
            else if (l == LockType.X) {
                this.xltid  = tid;
            }
        }

        public void setLockType (LockType l) {
            this.l = l; 
        }

        public void setXLockTransactionId (TransactionId tid) {
            this.xltid = tid; 
        }

        public void addSLockTransactionId (TransactionId tid) {
            sltids.add(tid);
        }

        public TransactionId getXLockTransactionId() {
            return this.xltid;
        }

        public ArrayList<TransactionId> getSLockTransactionIds() {
            return this.sltids;
        }

        public LockType getLockType() {
            return this.l;
        }

        public void upgrade(TransactionId tid) {
            //System.out.println("upgrade S lock to X lock");
            if (!sltids.contains(tid))
                System.out.println("Error: tid not in share lock list!");

            if (l != LockType.S)
                System.out.println("Error: not S lock! not able to upgrade!");
            l = LockType.X;
            this.xltid = tid;
            sltids.clear();
        }
    }

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.buffers = new ConcurrentHashMap<Integer, Page>(numPages);
        this.locks   = new ConcurrentHashMap<Integer, Lock>();
        this.pages   = numPages;
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        //System.out.println("buffer size:"+buffers.size());

        //There are many possible ways to detect deadlock. 
        //For example, you may implement a simple timeout policy that aborts a transaction if it has not completed after a given period of time. 
        //Alternately, you may implement cycle-detection in a dependency graph data structure. 
        //  In this scheme, you would check for cycles in a dependency graph whenever you attempt to grant a new lock, and abort something if a cycle exists.
        
        long lockStart = System.currentTimeMillis();
        //System.out.println(lockStart);

        // READ WRITE request
        if (perm == Permissions.READ_WRITE) {
            synchronized(xLock) {
                while (!holdsLock(tid, pid)) {
                    // deadlock detection
                    long lockWait = System.currentTimeMillis() - lockStart;
                    //System.out.println("wait X lock, tid:"+tid);
                    if (lockWait > 1000)
                        throw new TransactionAbortedException();

                    if (locks.containsKey(pid.hashCode())) {
                        Lock l = locks.get(pid.hashCode());
                        if ((l.getLockType() == Lock.LockType.NO_LOCK)) {

                            //System.out.println(pid.hashCode()+" READ WRITE; LockType:"+l.getLockType()+" lock X transactionId:"+l.getXLockTransactionId()+" request tid:"+tid);
                            l.setLockType(Lock.LockType.X);
                            l.setXLockTransactionId(tid);
                        }
                    }
                    else {
                        Lock l = new Lock(tid, Lock.LockType.X);
                        //System.out.println(pid.hashCode()+" fetch READ WRITE; LockType:"+l.getLockType()+" transactionId:"+l.getXLockTransactionId());
                        locks.put(pid.hashCode(), l);
                    }
                }

                // upgrade lock
                if (locks.get(pid.hashCode()).getLockType() == Lock.LockType.S && locks.get(pid.hashCode()).getSLockTransactionIds().contains(tid)) {
                    //discardPage(pid);
                    locks.get(pid.hashCode()).upgrade(tid);
                }
            }

        }
        // READ ONLY request
        else if (perm == Permissions.READ_ONLY) {
            synchronized(sLock) {
                //System.out.println("enter READ_ONLY");
                while (!holdsLock(tid, pid)) {
                    // deadlock detection
                    long lockWait = System.currentTimeMillis() - lockStart;
                    //System.out.println("wait S lock, tid:"+tid);
                    if (lockWait > 100)
                        throw new TransactionAbortedException();

                    if (locks.containsKey(pid.hashCode())) {
                        Lock l = locks.get(pid.hashCode());
                        if (l.getLockType() == Lock.LockType.NO_LOCK) {
                            l.setLockType(Lock.LockType.S);
                            l.addSLockTransactionId(tid);
                            //System.out.println(pid.hashCode()+" READ ONLY; LockType:"+l.getLockType()+" transactionId:"+l.getSLockTransactionIds()+" request tid:"+tid);
                        }
                        else if (l.getLockType() == Lock.LockType.S) {
                            l.addSLockTransactionId(tid);
                            //System.out.println(pid.hashCode()+" READ ONLY added; LockType:"+l.getLockType()+" transactionId:"+l.getSLockTransactionIds()+" request tid:"+tid);
                        }
                    }
                    else {
                        Lock l = new Lock(tid, Lock.LockType.S);
                        //System.out.println(pid.hashCode()+" fetch READ ONLY; LockType:"+l.getLockType()+" transactionId:"+l.getSLockTransactionIds());
                        //System.out.println(l);
                        locks.put(pid.hashCode(), l);
                    }
                }
            }
        }

        // pages
        //look up the page in the buffer pool
        //if exist, return
        if (buffers.containsKey(pid.hashCode())) {
            //System.out.println("Read from buffer pool:"+pid.hashCode()+" tid:"+tid);
            return buffers.get(pid.hashCode());
        }
        //if not present, add to the buffer pool, new page is added.
        else {
            Catalog ctlg = Database.getCatalog();
            DbFile f     = ctlg.getDatabaseFile(pid.getTableId());
            Page p       = f.readPage(pid);
            if (p != null) {
                while (buffers.size() == pages) {
                    evictPage();
                }
                //System.out.println("[debug]Fetch page from disk:"+pid.hashCode()+" tid:"+tid);

                buffers.put(pid.hashCode(), p);
                return p;
            }
            else {
                System.out.println("null page read");
                return null;
            }
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        Lock l = locks.get(pid.hashCode());
        if (l.getLockType() == Lock.LockType.X && l.getXLockTransactionId().equals(tid)) {
            l.setLockType(Lock.LockType.NO_LOCK);
            l.setXLockTransactionId(null);
        }
        else if (l.getLockType() == Lock.LockType.S && l.getSLockTransactionIds().contains(tid)) {
            System.out.println("release lock in releasePage");
            l.getSLockTransactionIds().remove(tid);
            if (l.getSLockTransactionIds().size() == 0) {
                l.setLockType(Lock.LockType.NO_LOCK);
            }
        }
        else {
            System.out.println("transaction id not match");
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        if (!locks.containsKey(p.hashCode())) {
            //System.out.println("not in locks table");
            return false;
        }
        else {
            Lock l = locks.get(p.hashCode());
            //System.out.println(l.getXLockTransactionId());
            // X lock
            if (l.getLockType() == Lock.LockType.X && l.getXLockTransactionId().equals(tid)) {
                //System.out.println(tid+" hold X lock");
                return true;
            }
            // S lock
            else if(l.getLockType() == Lock.LockType.S && l.getSLockTransactionIds().contains(tid)) {
                //System.out.println(tid+" hold S lock");
                return true;
            }
            // neither
            else {
                //System.out.println(tid+" not hold lock");
                return false;
            }
        }
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        // pages
        Iterator<Integer> keys = buffers.keySet().iterator();
        while (keys.hasNext()) {
            int key = keys.next();
            Page p  = buffers.get(key);
            //System.out.println("key:"+key+" page:"+p);
            if (commit) {
                if (p.isDirty() != null && p.isDirty().equals(tid)) {
                    //System.out.println(p);
                    //byte[] data = p.getPageData();
                    //for (byte b: data)
                    //    System.out.println("transactionComplete:"+b);

                    // add based on instructions of lab6 part 1
                    // use current page contents as the before-image
                    // for the next transaction that modifies this page.
                    p.setBeforeImage();

                    flushPage(p.getId());
                }
            }
            else {
                if (p.isDirty() != null && p.isDirty().equals(tid)) {
                    discardPage(p.getId());
                    Catalog ctlg = Database.getCatalog();
                    DbFile f     = ctlg.getDatabaseFile(p.getId().getTableId());
                    buffers.put(key, f.readPage(p.getId()));
                }
            }
        }
        // locks
        keys = locks.keySet().iterator();
        while (keys.hasNext()) {
            int key = keys.next();
            Lock l = locks.get(key);
            // release all the lock related to tid
            //System.out.println("Transaction Id:"+l.getTransactionId());
            if (l.getLockType() == Lock.LockType.X && l.getXLockTransactionId().equals(tid)) {
                //System.out.println("release X lock: tid:"+tid);
                l.setLockType(Lock.LockType.NO_LOCK);
                l.setXLockTransactionId(null);
            }
            else if (l.getLockType() == Lock.LockType.S && l.getSLockTransactionIds().contains(tid)) {
                //System.out.println("release S lock: tid:"+tid);
                l.getSLockTransactionIds().remove(tid);
                if (l.getSLockTransactionIds().size() == 0) {
                    //System.out.println("release S lock: no shared lock");
                    l.setLockType(Lock.LockType.NO_LOCK);
                }
            }
            else {
                if (commit) {
                String error=" ";
                    if (l.getLockType() == Lock.LockType.X)
                        error += "type:"+l.getLockType()+" X tid:"+ l.getXLockTransactionId();
                    if (l.getLockType() == Lock.LockType.S)
                        error += "type:"+l.getLockType()+" S tid:"+ l.getSLockTransactionIds();
                    //System.out.println("lock type not correct! lock:"+l+" request tid:"+tid+error);
                    //System.out.println("Printing stack trace:");
                    //StackTraceElement[] elements = Thread.currentThread().getStackTrace();
                    //for (int i = 1; i < elements.length; i++) {
                    //    StackTraceElement s = elements[i];
                    //    System.out.println("\tat " + s.getClassName() + "." + s.getMethodName()
                    //    + "(" + s.getFileName() + ":" + s.getLineNumber() + ")");
                    //}
                }
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        Catalog ctlg = Database.getCatalog();
        DbFile f  = ctlg.getDatabaseFile(tableId);
        ArrayList<Page> dirtyPages = f.insertTuple(tid, t);
        for (Page dirtyPage : dirtyPages) {
            dirtyPage.markDirty(true, tid);
            buffers.put(dirtyPage.getId().hashCode(), dirtyPage);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableId  = t.getRecordId().getPageId().getTableId();
        Catalog ctlg = Database.getCatalog();
        DbFile f  = ctlg.getDatabaseFile(tableId);
        ArrayList<Page> dirtyPages = f.deleteTuple(tid, t);
        for (Page dirtyPage : dirtyPages) {
            dirtyPage.markDirty(true, tid);
            buffers.put(dirtyPage.getId().hashCode(), dirtyPage);
        }

        //System.out.println("bufferpool.java:"+this);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        Iterator<Integer> keys = buffers.keySet().iterator();
        while (keys.hasNext()) {
           int key = keys.next();
           Page p  = buffers.get(key);
           flushPage(p.getId());
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        //System.out.println("remove page:"+pid.hashCode());
        buffers.remove(pid.hashCode());
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Catalog ctlg = Database.getCatalog();
        DbFile f     = ctlg.getDatabaseFile(pid.getTableId());
        Page p       =  buffers.get(pid.hashCode());
        //System.out.println("flushPage:"+pid.getPageNumber());
        TransactionId tid = p.isDirty();
        if (tid != null) {
            //System.out.println(tid);

            // add based on instructions of lab6 part 1
            // append an update record to the log, with 
            // a before-image and after-image.
            Database.getLogFile().logWrite(tid, p.getBeforeImage(), p);
            Database.getLogFile().force();

            f.writePage(p);
            p.markDirty(false, tid);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        //System.out.println("evict page");
        Iterator<Integer> keys = buffers.keySet().iterator();
        while (keys.hasNext()) {
           int key = keys.next();
           Page p  = buffers.get(key);
           //System.out.println("pid:"+p.getId().hashCode()+" tid:"+p.isDirty());
           //if (p.isDirty() != null) {
           //    //System.out.println("key:"+key);
           //    flushPage(p.getId());
           //    buffers.remove(p.getId().hashCode());
           //    break;
           //}
           //else {
           //    discardPage(p.getId());
           //    break;
           //}

           // Implement NO STEAL
           if (p.isDirty() == null) {
               discardPage(p.getId());
               return;
           }
        }
        throw new DbException("All pages are dirty in buffer pool!");
    }

}
