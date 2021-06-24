package nova.hetu.omniruntime.constant;

import nova.hetu.omniruntime.constants.AggType;
import nova.hetu.omniruntime.constants.Status;
import nova.hetu.omniruntime.constants.VecType;
import org.testng.annotations.Test;

import static nova.hetu.omniruntime.constants.AggType.OMNI_AGGREGATION_TYPE_SUM;
import static nova.hetu.omniruntime.constants.Status.OMNI_STATUS_ERROR;
import static nova.hetu.omniruntime.constants.Status.OMNI_STATUS_FINISHED;
import static nova.hetu.omniruntime.constants.Status.OMNI_STATUS_NORMAL;
import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_DOUBLE;
import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_INT;
import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_LONG;
import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_VARCHAR;
import static org.testng.Assert.assertEquals;

public class ConstantLoadTest
{
    @Test
    public void testLoadConstant()
    {
        Status status = OMNI_STATUS_NORMAL;
        assertEquals(status.getValue(), 0);
        status = OMNI_STATUS_ERROR;
        assertEquals(status.getValue(), -1);
        status = OMNI_STATUS_FINISHED;
        assertEquals(status.getValue(), 1);

        VecType type = OMNI_VEC_TYPE_INT;
        assertEquals(type.getValue(), 1);
        type = OMNI_VEC_TYPE_LONG;
        assertEquals(type.getValue(), 2);
        type = OMNI_VEC_TYPE_DOUBLE;
        assertEquals(type.getValue(), 3);
        type = OMNI_VEC_TYPE_VARCHAR;
        assertEquals(type.getValue(), 100);

        AggType aggType = OMNI_AGGREGATION_TYPE_SUM;
        assertEquals(aggType.getValue(), 0);
    }

    @Test
    public void testEquals()
    {
        Status status = OMNI_STATUS_NORMAL;
        assertEquals(status.getValue(), 0);
    }
}
