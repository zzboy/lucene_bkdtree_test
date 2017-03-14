import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.BitSet;
import java.util.Random;

import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.bkd.BKDReader;
import org.apache.lucene.util.bkd.BKDWriter;
import org.apache.lucene.util.bkd.LeafBlockSizeStat;

class LongRangeQueryVistor implements IntersectVisitor
{
    private long min_, max_;
    private boolean minclude_, maxclude_;
    public BitSet hits_;
    public int hits_count_ = 0;
    public LongRangeQueryVistor(final long min, final boolean minclude, final long max, final boolean maxclude, final int bits)
    {
        this.min_ = min;
        this.minclude_ = minclude;
        this.max_ = max;
        this.maxclude_ = maxclude;
        hits_ = new BitSet(bits);
    }
    public static boolean ValueInRange(final long min, final boolean minclude, final long max, final boolean maxclude, final long value)
    {
        if(!(minclude && value >= min) && !(!minclude && value > min)) return false;
        if(!(maxclude && value <= max) && !(!maxclude && value < max)) return false;
        return true;
    }
    @Override
    public void visit(int docID) throws IOException {
        if(!hits_.get(docID)){ hits_.set(docID); ++hits_count_; }
    }

    @Override
    public void visit(int docID, byte[] packedValue) throws IOException {
        long value = NumericUtils.sortableBytesToLong(packedValue, 0);
        if(ValueInRange(min_, minclude_, max_, maxclude_, value)) if(!hits_.get(docID)){ hits_.set(docID); ++hits_count_; }
    }

    @Override
    public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
        long min = NumericUtils.sortableBytesToLong(minPackedValue, 0);
        long max =NumericUtils.sortableBytesToLong(maxPackedValue, 0);
        boolean minin = ValueInRange(min_, minclude_, max_, maxclude_, min);
        boolean maxin = ValueInRange(min_, minclude_, max_, maxclude_, max);
        if((minin & maxin) == true) return Relation.CELL_INSIDE_QUERY;
        if((minin ^ maxin) == true) return Relation.CELL_CROSSES_QUERY;
        if((minclude_ && max < min_) || (!minclude_ && max <= min_)) return Relation.CELL_OUTSIDE_QUERY;
        if((maxclude_ && min > max_) || (!maxclude_ && min >= max_)) return Relation.CELL_OUTSIDE_QUERY;
        return Relation.CELL_CROSSES_QUERY;
    }
};

class DoubleRangeQueryVistor implements IntersectVisitor
{
    private double min_, max_;
    private boolean minclude_, maxclude_;
    public BitSet hits_;
    public int hits_count_ = 0;
    public DoubleRangeQueryVistor(final double min, final boolean minclude, final double max, final boolean maxclude, final int bits)
    {
        this.min_ = min;
        this.minclude_ = minclude;
        this.max_ = max;
        this.maxclude_ = maxclude;
        hits_ = new BitSet(bits);
    }
    public static boolean ValueInRange(final double min, final boolean minclude, final double max, final boolean maxclude, final double value)
    {
        if(!(minclude && value >= min) && !(!minclude && value > min)) return false;
        if(!(maxclude && value <= max) && !(!maxclude && value < max)) return false;
        return true;
    }
    @Override
    public void visit(int docID) throws IOException {
        if(!hits_.get(docID)){ hits_.set(docID); ++hits_count_; }
    }

    @Override
    public void visit(int docID, byte[] packedValue) throws IOException {
        double value = NumericUtils.sortableLongToDouble(NumericUtils.sortableBytesToLong(packedValue, 0));
        if(ValueInRange(min_, minclude_, max_, maxclude_, value)) if(!hits_.get(docID)){ hits_.set(docID); ++hits_count_; }
    }

    @Override
    public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
        double min = NumericUtils.sortableLongToDouble(NumericUtils.sortableBytesToLong(minPackedValue, 0));
        double max = NumericUtils.sortableLongToDouble(NumericUtils.sortableBytesToLong(maxPackedValue, 0));
        boolean minin = ValueInRange(min_, minclude_, max_, maxclude_, min);
        boolean maxin = ValueInRange(min_, minclude_, max_, maxclude_, max);
        if((minin & maxin) == true) return Relation.CELL_INSIDE_QUERY;
        if((minin ^ maxin) == true) return Relation.CELL_CROSSES_QUERY;
        if((minclude_ && max < min_) || (!minclude_ && max <= min_)) return Relation.CELL_OUTSIDE_QUERY;
        if((maxclude_ && min > max_) || (!maxclude_ && min >= max_)) return Relation.CELL_OUTSIDE_QUERY;
        return Relation.CELL_CROSSES_QUERY;
    }
};

public class BkdTreeTest {
    public static void LongRangeQuery(int docNum) throws IOException {
        String dirstr = "./bkdtree_test";
        Directory dir = FSDirectory.open(Paths.get(dirstr));
        int maxPointsInLeafNode = 4096;
        Random rand = new Random();
        int totalPointCount = docNum;
        long data[] = new long[docNum];
        for(int i = 0; i < docNum; ++i) data[i] = rand.nextLong();
        String indexFileName = "random_long.bkd";
        new File(dirstr + File.separator + indexFileName).delete();
        long startTime=System.currentTimeMillis();
        BKDWriter wrt = new BKDWriter(docNum, dir, "test", 1, Long.BYTES, maxPointsInLeafNode, 512, totalPointCount, true);
        for(int d = 0; d < docNum; ++d) {
            byte []packed = new byte[Long.BYTES];
            NumericUtils.longToSortableBytes(data[d], packed, 0);
            wrt.add(packed, d);
        }
        IndexOutput indexOutput = dir.createOutput(indexFileName, new IOContext());
        long indexFP = wrt.finish(indexOutput);
        wrt.close();
        dir.close();
        System.out.println("tree size: " + indexOutput.getFilePointer());
        System.out.println("doc block size: " + LeafBlockSizeStat.docBlockSize);
        System.out.println("value block size: " + LeafBlockSizeStat.valueBlockSize);
        indexOutput.close();
        System.out.println("tree build time: "+ (System.currentTimeMillis() - startTime)+" ms");   
        Directory readerDir = FSDirectory.open(Paths.get(dirstr));
        IndexInput indexInput = readerDir.openInput(indexFileName, new IOContext());
        indexInput.seek(indexFP);
        BKDReader reader = new BKDReader(indexInput);
        System.out.println("min: " + NumericUtils.sortableBytesToLong(reader.getMinPackedValue(), 0) + ", max: " + NumericUtils.sortableBytesToLong(reader.getMaxPackedValue(), 0));
        int searchtimes = 100;
        float avgSearchTime = 0;
        for(int i = 0; i < searchtimes; ++i){
            long min = rand.nextLong(), max = rand.nextLong();
            if(min > max){
                long tmp = min;
                min = max;
                max = tmp;
            }
            boolean minclude = ((rand.nextInt() & 0x7FFFFFFF) % 2) == 1, maxclude = ((rand.nextInt() & 0x7FFFFFFF) % 2) == 1;
            startTime = System.currentTimeMillis();
            LongRangeQueryVistor vistor = new LongRangeQueryVistor(min, minclude, max, maxclude, docNum);
            reader.intersect(vistor);
            avgSearchTime += (System.currentTimeMillis() - startTime);
            int memhits = 0;
            for(int d = 0; d < docNum; ++d){
                if(LongRangeQueryVistor.ValueInRange(min, minclude, max, maxclude, data[d])) ++memhits;
            }
            if(vistor.hits_count_ != memhits) System.err.println("hit from source data: " + memhits);
        }
        System.out.println("tree search time: "+ (avgSearchTime / searchtimes) +" ms");
    }
    public static void DoubleRangeQuery(int docNum) throws IOException {
        String dirstr = "./bkdtree_test";
        Directory dir = FSDirectory.open(Paths.get(dirstr));
        int maxPointsInLeafNode = 4096;
        Random rand = new Random();
        int totalPointCount = docNum;
        double data[] = new double[docNum];
        for(int i = 0; i < docNum; ++i) data[i] = rand.nextDouble();
        String indexFileName = "random_double.bkd";
        new File(dirstr + File.separator + indexFileName).delete();
        long startTime=System.currentTimeMillis();
        BKDWriter wrt = new BKDWriter(docNum, dir, "test", 1, Long.BYTES, maxPointsInLeafNode, 512, totalPointCount, true);
        for(int d = 0; d < docNum; ++d) {
            byte []packed = new byte[Double.BYTES];
            NumericUtils.longToSortableBytes(NumericUtils.doubleToSortableLong(data[d]), packed, 0);
            wrt.add(packed, d);
        }
        IndexOutput indexOutput = dir.createOutput(indexFileName, new IOContext());
        long indexFP = wrt.finish(indexOutput);
        wrt.close();
        dir.close();
        System.out.println("tree size: " + indexOutput.getFilePointer());
        System.out.println("doc block size: " + LeafBlockSizeStat.docBlockSize);
        System.out.println("value block size: " + LeafBlockSizeStat.valueBlockSize);
        indexOutput.close();
        System.out.println("tree build time: "+ (System.currentTimeMillis() - startTime)+" ms");   
        Directory readerDir = FSDirectory.open(Paths.get(dirstr));
        IndexInput indexInput = readerDir.openInput(indexFileName, new IOContext());
        indexInput.seek(indexFP);
        BKDReader reader = new BKDReader(indexInput);
        System.out.println("min: " + NumericUtils.sortableLongToDouble(NumericUtils.sortableBytesToLong(reader.getMinPackedValue(), 0)) + ", max: " + NumericUtils.sortableLongToDouble(NumericUtils.sortableBytesToLong(reader.getMaxPackedValue(), 0)));
        int searchtimes = 100;
        float avgSearchTime = 0;
        for(int i = 0; i < searchtimes; ++i){
            double min = rand.nextDouble(), max = rand.nextDouble();
            if(min > max){
                double tmp = min;
                min = max;
                max = tmp;
            }
            boolean minclude = ((rand.nextInt() & 0x7FFFFFFF) % 2) == 1, maxclude = ((rand.nextInt() & 0x7FFFFFFF) % 2) == 1;
            startTime = System.currentTimeMillis();
            DoubleRangeQueryVistor vistor = new DoubleRangeQueryVistor(min, minclude, max, maxclude, docNum);
            reader.intersect(vistor);
            avgSearchTime += (System.currentTimeMillis() - startTime);
            int memhits = 0;
            for(int d = 0; d < docNum; ++d){
                if(DoubleRangeQueryVistor.ValueInRange(min, minclude, max, maxclude, data[d])) ++memhits;
            }
            if(vistor.hits_count_ != memhits) System.err.println("hit from source data: " + memhits);
        }
        System.out.println("tree search time: "+ (avgSearchTime / searchtimes) +" ms");
    }
    public static void StatusRangeQuery(int docNum) throws IOException {
        String dirstr = "./bkdtree_test";
        Directory dir = FSDirectory.open(Paths.get(dirstr));
        int maxPointsInLeafNode = 4096;
        int totalPointCount = docNum;
        long data[] = new long[docNum];
        BufferedReader freader = new BufferedReader(new FileReader("./bkdtree_test/status.dat"));
        String status[] = freader.readLine().split(",");
        freader.close();
        for(int i = 0; i < docNum; ++i) data[i] = Long.parseLong(status[i]);
        String indexFileName = "status.bkd";
        new File(dirstr + File.separator + indexFileName).delete();
        long startTime=System.currentTimeMillis();
        BKDWriter wrt = new BKDWriter(docNum, dir, "test", 1, Long.BYTES, maxPointsInLeafNode, 512, totalPointCount, true);
        for(int d = 0; d < docNum; ++d) {
            byte []packed = new byte[Long.BYTES];
            NumericUtils.longToSortableBytes(data[d], packed, 0);
            wrt.add(packed, d);
        }
        IndexOutput indexOutput = dir.createOutput(indexFileName, new IOContext());
        long indexFP = wrt.finish(indexOutput);
        wrt.close();
        dir.close();
        System.out.println("tree size: " + indexOutput.getFilePointer());
        System.out.println("doc block size: " + LeafBlockSizeStat.docBlockSize);
        System.out.println("value block size: " + LeafBlockSizeStat.valueBlockSize);
        indexOutput.close();
        System.out.println("tree build time: "+ (System.currentTimeMillis() - startTime)+" ms");   
        Directory readerDir = FSDirectory.open(Paths.get(dirstr));
        IndexInput indexInput = readerDir.openInput(indexFileName, new IOContext());
        indexInput.seek(indexFP);
        BKDReader reader = new BKDReader(indexInput);
        System.out.println("min: " + NumericUtils.sortableBytesToLong(reader.getMinPackedValue(), 0) + ", max: " + NumericUtils.sortableBytesToLong(reader.getMaxPackedValue(), 0));
        int searchtimes = 100;
        float avgSearchTime = 0;
        Random rand = new Random();
        for(int i = 0; i < searchtimes; ++i){
            long min = rand.nextLong() % 600, max = rand.nextLong() % 600;
            if(min > max){
                long tmp = min;
                min = max;
                max = tmp;
            }
            boolean minclude = ((rand.nextInt() & 0x7FFFFFFF) % 2) == 1, maxclude = ((rand.nextInt() & 0x7FFFFFFF) % 2) == 1;
            startTime = System.currentTimeMillis();
            LongRangeQueryVistor vistor = new LongRangeQueryVistor(min, minclude, max, maxclude, docNum);
            reader.intersect(vistor);
            avgSearchTime += (System.currentTimeMillis() - startTime);
            int memhits = 0;
            for(int d = 0; d < docNum; ++d){
                if(LongRangeQueryVistor.ValueInRange(min, minclude, max, maxclude, data[d])) ++memhits;
            }
            if(vistor.hits_count_ != memhits) System.err.println("hit from source data: " + memhits);
        }
        System.out.println("tree search time: "+ (avgSearchTime / searchtimes) +" ms");
    }
    public static void main(String []args) throws IOException
    {
        System.out.println("------------------------------------");
        LongRangeQuery(2000000);
        System.out.println("------------------------------------");
        DoubleRangeQuery(2000000);
        System.out.println("------------------------------------");
        StatusRangeQuery(9999 * 199);
        System.out.println("------------------------------------");
    }
}


