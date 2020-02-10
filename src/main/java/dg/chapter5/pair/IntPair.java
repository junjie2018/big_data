package dg.chapter5.pair;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntPair implements WritableComparable<IntPair> {

    private int first;
    private int second;

    public IntPair() {
    }

    public IntPair(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public int getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    @Override
    public int compareTo(IntPair ip) {
        int cmp = Integer.compare(first, ip.first);
        if (cmp != 0) {
            return cmp;
        }
        return Integer.compare(second, ip.second);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(first);
        out.writeInt(second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readInt();
        second = in.readInt();
    }

    @Override
    public int hashCode() {
        return first * 163 + second;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof IntPair) {
            IntPair ip = (IntPair) obj;
            return first == ip.first && second == ip.second;
        }
        return false;
    }

    public static int compare(int a, int b) {
        return (Integer.compare(a, b));
    }

    @Override
    public String toString() {
        return first + "\t" + second;
    }
}
