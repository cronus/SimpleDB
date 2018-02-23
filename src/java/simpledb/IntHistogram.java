package simpledb;

import java.lang.*;

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
        this.step    = ((double) max - min) / buckets;
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


        //System.out.println("min:"+min+" max:"+max+" v:"+v);
        double selectivity = 0;
        int width;
        int bucketno = (int) ((double)(v - min) / step);


        if (v == max) {
            bucketno -= 1;
        }

        double left  = min + bucketno * step;
        double right = min + (bucketno + 1) * step;

        width = (int) (Math.floor(right) - Math.ceil(left)) + 1;

        //System.out.println("left:"+left+" right:"+right+" width:"+width);

        if (op == Predicate.Op.EQUALS) {
            //System.out.println("bucket no:"+bucketno+" cnt:"+bucketCount[bucketno]+" total:"+totalCount+" step:"+step);
            selectivity = (double) bucketCount[bucketno] / (totalCount * width);
            //System.out.println("equal case selectivity:"+selectivity);
        }
        else if (op == Predicate.Op.NOT_EQUALS) {
            selectivity = 1 - (double) bucketCount[bucketno] / (totalCount * width);
        }
        else if (op == Predicate.Op.GREATER_THAN) {
            if (v < min) {
                return 1.0;
            }
            else if (v > max) {
                return 0.0;
            }
            //System.out.println("bucket no:"+bucketno+" cnt:"+bucketCount[bucketno]+" total:"+totalCount+" step:"+step);
            selectivity += (rightBrdy[bucketno] - v) * bucketCount[bucketno] / (totalCount * width);
            for (int i = bucketno + 1; i < buckets; i++) {
                selectivity += (double) bucketCount[i] / totalCount;      
            }
            //System.out.println("greater than case selectivity:"+selectivity);
        }
        else if (op == Predicate.Op.GREATER_THAN_OR_EQ) {
            if (v < min) {
                return 1.0;
            }
            else if (v > max) {
                return 0.0;
            }
            //System.out.println("bucket no:"+bucketno+" cnt:"+bucketCount[bucketno]+" total:"+totalCount+" step:"+step);
            selectivity += (rightBrdy[bucketno] - v + 1) * bucketCount[bucketno] / (totalCount * width);
            for (int i = bucketno + 1; i < buckets; i++) {
                selectivity += (double) bucketCount[i] / totalCount;      
            }
            //System.out.println("greater than or eq case selectivity:"+selectivity);
        }
        else if (op == Predicate.Op.LESS_THAN) {
            if (v > max) {
                return 1.0;
            }
            else if (v < min) {
                return 0.0;
            }
            selectivity += (v - leftBrdy[bucketno]) * bucketCount[bucketno] / (totalCount * width);
            for (int i = bucketno - 1; i >= 0; i--) {
                selectivity += (double) bucketCount[i] / totalCount;      
            }
            //System.out.println("less than case selectivity:"+selectivity);
        }
        else if (op == Predicate.Op.LESS_THAN_OR_EQ) {
            if (v > max) {
                return 1.0;
            }
            else if (v < min) {
                return 0.0;
            }
            selectivity += (v - leftBrdy[bucketno] + 1) * bucketCount[bucketno] / (totalCount * width);
            for (int i = bucketno - 1; i >= 0; i--) {
                selectivity += (double) bucketCount[i] / totalCount;      
            }
            //System.out.println("less than or eq case selectivity:"+selectivity);
        }
        return selectivity;
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
        double avg = 0;
        for (int i = min; i <= max; i++) {
            avg += estimateSelectivity(Predicate.Op.EQUALS, i); 
        }
        return avg;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        String intHistogramStr = "";
        for (int i = 0; i < buckets; i++) {
            intHistogramStr += leftBrdy[i]+":"+rightBrdy[i]+"\t"+bucketCount[i]+"\n";
        }
        return intHistogramStr;
    }
}
