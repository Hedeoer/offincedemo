package cn.hedeoer.udaf;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Assert;
import org.junit.Test;

public class EventRateUDAFTest {

    private static final TypeInfo[] STRING_TYPES = new TypeInfo[]{
            TypeInfoFactory.stringTypeInfo,
            TypeInfoFactory.stringTypeInfo,
            TypeInfoFactory.stringTypeInfo
    };

    private static final ObjectInspector[] STRING_INSPECTORS = new ObjectInspector[]{
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaStringObjectInspector
    };

    @Test
    public void shouldCalculateRateInCompleteMode() throws Exception {
        GenericUDAFEvaluator evaluator = new EventRateUDAF().getEvaluator(STRING_TYPES);
        evaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, STRING_INSPECTORS);
        GenericUDAFEvaluator.AggregationBuffer buffer = evaluator.getNewAggregationBuffer();

        evaluator.iterate(buffer, new Object[]{"view", "pay", "view"});
        evaluator.iterate(buffer, new Object[]{"pay", "pay", "view"});
        evaluator.iterate(buffer, new Object[]{"click", "pay", "view"});
        evaluator.iterate(buffer, new Object[]{"view", "pay", "view"});

        Assert.assertEquals(0.5D, (Double) evaluator.terminate(buffer), 0.000001D);
    }

    @Test
    public void shouldMergePartialResultsInFinalMode() throws Exception {
        ObjectInspector partialInspector;
        Object partial1;
        Object partial2;

        GenericUDAFEvaluator partialEvaluator1 = new EventRateUDAF().getEvaluator(STRING_TYPES);
        partialInspector = partialEvaluator1.init(GenericUDAFEvaluator.Mode.PARTIAL1, STRING_INSPECTORS);
        GenericUDAFEvaluator.AggregationBuffer partialBuffer1 = partialEvaluator1.getNewAggregationBuffer();
        partialEvaluator1.iterate(partialBuffer1, new Object[]{"view", "pay", "view"});
        partialEvaluator1.iterate(partialBuffer1, new Object[]{"pay", "pay", "view"});
        partial1 = partialEvaluator1.terminatePartial(partialBuffer1);

        GenericUDAFEvaluator partialEvaluator2 = new EventRateUDAF().getEvaluator(STRING_TYPES);
        partialEvaluator2.init(GenericUDAFEvaluator.Mode.PARTIAL1, STRING_INSPECTORS);
        GenericUDAFEvaluator.AggregationBuffer partialBuffer2 = partialEvaluator2.getNewAggregationBuffer();
        partialEvaluator2.iterate(partialBuffer2, new Object[]{"view", "pay", "view"});
        partialEvaluator2.iterate(partialBuffer2, new Object[]{"click", "pay", "view"});
        partial2 = partialEvaluator2.terminatePartial(partialBuffer2);

        GenericUDAFEvaluator finalEvaluator = new EventRateUDAF().getEvaluator(STRING_TYPES);
        finalEvaluator.init(GenericUDAFEvaluator.Mode.FINAL, new ObjectInspector[]{partialInspector});
        GenericUDAFEvaluator.AggregationBuffer finalBuffer = finalEvaluator.getNewAggregationBuffer();
        finalEvaluator.merge(finalBuffer, partial1);
        finalEvaluator.merge(finalBuffer, partial2);

        Assert.assertEquals(0.5D, (Double) finalEvaluator.terminate(finalBuffer), 0.000001D);
    }
}
