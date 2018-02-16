package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;
    private int min;
    private int max;
    private double step;
    private int totalCount;

    private int[] bucketCount;
    private double[] leftBrdy;
    private double[] rightBrdy;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        this.min     = min;
        this.max     = max;
        this.step    = (double)(max - min) / buckets;
        this.totalCount = 0;

        this.bucketCount  = new int[buckets];
        this.leftBrdy     = new double[buckets];
        this.rightBrdy    = new double[buckets];

        for (int i = 0; i < buckets; i++) {
            leftBrdy[i]  = min + i * step;
            rightBrdy[i] = min + (i + 1) * step;
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        totalCount++;
        //System.out.println("bucket:"+buckets);
        //System.out.println("v:"+v+" min:"+min+" step:"+step);
        if (v == max) {
            bucketCount[buckets - 1]++;
        }
        else {
            int bucketno = (int) ((double)(v - min) / step);
            //System.out.println(bucketno);
            bucketCount[bucketno]++;
        }
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        //return -1.0;
        double selectivity = 0;
        for (int i = 0; i < buckets; i++) {
            if (leftBrdy[i] < v && v < rightBrdy[i]) {
                if (op == Predicate.Op.EQUALS) {
                    return bucketCount[i] / (totalCount * step);
                }
                else if (op == Predicate.Op.GREATER_THAN) {
                    selectivity += (rightBrdy[i] - v) * bucketCount[i] / totalCount;
                    for (int j = i + 1; j < buckets; j++) {
                        selectivity += bucketCount[j] / totalCount;      
                    }
                    return selectivity;
                }
                else if (op == Predicate.Op.LESS_THAN) {
                    selectivity += (v - leftBrdy[i]) * bucketCount[i] / totalCount;
                    for (int j = i - 1; j >= 0; j--) {
                        selectivity += bucketCount[j] / totalCount;      
                    }
                    return selectivity;
                }
            }
        }
        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
