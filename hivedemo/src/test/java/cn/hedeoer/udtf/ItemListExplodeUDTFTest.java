package cn.hedeoer.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.Collector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ItemListExplodeUDTFTest {

    @Test
    public void shouldExplodeItemListIntoRows() throws Exception {
        ItemListExplodeUDTF udtf = new ItemListExplodeUDTF();
        StructObjectInspector inspector = udtf.initialize(new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.javaStringObjectInspector
        });

        List<String> fieldNames = new ArrayList<>();
        for (StructField field : inspector.getAllStructFieldRefs()) {
            fieldNames.add(field.getFieldName());
        }
        Assert.assertEquals(Arrays.asList("sku_id", "qty", "price"), fieldNames);

        CollectingCollector collector = new CollectingCollector();
        udtf.setCollector(collector);

        udtf.process(new Object[]{"sku01:1:2999,sku02:2:1599"});
        udtf.close();

        Assert.assertEquals(2, collector.rows.size());
        Assert.assertArrayEquals(new Object[]{"sku01", 1, 2999}, collector.rows.get(0));
        Assert.assertArrayEquals(new Object[]{"sku02", 2, 1599}, collector.rows.get(1));
    }

    @Test(expected = UDFArgumentLengthException.class)
    public void shouldRejectInvalidArgumentCount() throws UDFArgumentException {
        ItemListExplodeUDTF udtf = new ItemListExplodeUDTF();
        udtf.initialize(new ObjectInspector[]{});
    }

    @Test(expected = UDFArgumentException.class)
    public void shouldRejectNonStringArgument() throws UDFArgumentException {
        ItemListExplodeUDTF udtf = new ItemListExplodeUDTF();
        udtf.initialize(new ObjectInspector[]{
                PrimitiveObjectInspectorFactory.javaIntObjectInspector
        });
    }

    private static class CollectingCollector implements Collector {
        private final List<Object[]> rows = new ArrayList<>();

        @Override
        public void collect(Object input) {
            Object[] row = (Object[]) input;
            rows.add(Arrays.copyOf(row, row.length));
        }
    }
}
