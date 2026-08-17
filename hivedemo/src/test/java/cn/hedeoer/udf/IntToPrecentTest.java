package cn.hedeoer.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;

public class IntToPrecentTest {

    @Test
    public void shouldFormatIntegerRatioAsPercent() throws HiveException {
        IntToPrecent udf = new IntToPrecent();
        ObjectInspector returnInspector = udf.initialize(new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.javaIntObjectInspector,
                PrimitiveObjectInspectorFactory.javaIntObjectInspector
        });

        Assert.assertEquals(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                returnInspector
        );

        Object result = udf.evaluate(new GenericUDF.DeferredObject[]{
                new GenericUDF.DeferredJavaObject(1),
                new GenericUDF.DeferredJavaObject(4)
        });

        Assert.assertEquals("25%", result);
    }

    @Test(expected = UDFArgumentLengthException.class)
    public void shouldRejectInvalidArgumentCount() throws UDFArgumentException {
        IntToPrecent udf = new IntToPrecent();

        udf.initialize(new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.javaIntObjectInspector
        });
    }

    @Test(expected = UDFArgumentException.class)
    public void shouldRejectNonIntegerArgument() throws UDFArgumentException {
        IntToPrecent udf = new IntToPrecent();

        udf.initialize(new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaIntObjectInspector
        });
    }
}
