package cn.hedeoer.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.text.DecimalFormat;

public class IntToPrecent extends GenericUDF {
    // 处理逻辑
    public String intToPrecent(String i1, String i2){
        int i = Integer.parseInt(i1);
        int j = Integer.parseInt(i2);
        double result = (double)i/j;
        DecimalFormat df = new DecimalFormat("0%");
        return df.format(result);
    }

    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        // 判断传入的参数个数
        if(objectInspectors.length != 2){
            throw new UDFArgumentLengthException("Input Args Length Error !!!");
        }
        // 判断传入参数的类型
        if (!objectInspectors[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)
                || !PrimitiveObjectInspector.PrimitiveCategory.INT.equals(((PrimitiveObjectInspector)objectInspectors[0]).getPrimitiveCategory())){
            throw new UDFArgumentException("函数第一个参数为int类型"); // 当自定义UDF参数与预期不符时，抛出异常
        }
        if (!objectInspectors[1].getCategory().equals(ObjectInspector.Category.PRIMITIVE)
                || !PrimitiveObjectInspector.PrimitiveCategory.INT.equals(((PrimitiveObjectInspector)objectInspectors[1]).getPrimitiveCategory())){
            throw new UDFArgumentException("函数第二个参数为int类型");
        }
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    public String evaluate(DeferredObject[] deferredObjects) throws HiveException {
        String num1 = deferredObjects[0].get().toString();
        String num2 = deferredObjects[1].get().toString();
        return intToPrecent(num1,num2);
    }

    public String getDisplayString(String[] strings) {
        // 生成HQL explain子句中显示的日志
        return strings[0];
    }
}
