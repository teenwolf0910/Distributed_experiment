package split;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author ljw
 * @create 2020/5/11 9:21
 */
public class SplitBean implements Writable {

    private  String a;
    private  String b;
    private  String n;

    //反序列化，需要空参构造函数
    public SplitBean(){
        super();
    }
    //带参构造函数
    public SplitBean(String a, String b, String   n){
        super();
        this.a=a;
        this.b=b;
        this.n=n;
    }
    //写序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(a);
        dataOutput.writeUTF(b);
        dataOutput.writeUTF(n);
    }
    //反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.a=dataInput.readUTF();
        this.b=dataInput.readUTF();
        this.n=dataInput.readUTF();
    }

    //编写tostring方法，方便打印
    @Override
    public  String toString(){
        return  a+"\t"+b+"\t"+n;
    }

    public String getA() {
        return a;
    }

    public void setA(String a) {
        this.a = a;
    }

    public String getB() {
        return b;
    }

    public void setB(String b) {
        this.b = b;
    }

    public String getN() {
        return n;
    }

    public void setN(String n) {
        this.n = n;
    }

    public  void set(String a, String b, String n){
        this.a=a;
        this.b=b;
        this.n=n;
    }
}
