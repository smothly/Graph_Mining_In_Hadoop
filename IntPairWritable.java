package bigdata;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class IntPairWritable implements WritableComparable<IntPairWritable>{

    int u, v;

    public void set(int u, int v){
        this.u = u;
        this.v = v;
    }

    public void write(DataOutput out) throws IOException{
        out.writeInt(u);
        out.writeInt(v);
    }

    public void readFields(DataInput in) throws IOException{
        u = in.readInt();
        v = in.readInt();
    }

    public int compareTo(IntPairWritable o){
        if(this.u != o.u) return Integer.compare(this.u, o.u);
        return Integer.compare(this.v, o.v);
    }

    @Override
    public String toString() {
        return u + "\t" + v;
    }
}

