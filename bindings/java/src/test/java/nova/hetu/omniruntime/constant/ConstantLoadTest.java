package nova.hetu.omniruntime.constant;

import static nova.hetu.omniruntime.constants.AggType.OMNI_AGGREGATION_TYPE_SUM;
import static nova.hetu.omniruntime.constants.Status.OMNI_STATUS_ERROR;
import static nova.hetu.omniruntime.constants.Status.OMNI_STATUS_FINISHED;
import static nova.hetu.omniruntime.constants.Status.OMNI_STATUS_NORMAL;
import static org.testng.Assert.assertEquals;

import nova.hetu.omniruntime.constants.AggType;
import nova.hetu.omniruntime.constants.Status;

import org.testng.annotations.Test;

/**
 * The type Constant load test.
 */
public class ConstantLoadTest {
    /**
     * Test load constant.
     */
    @Test
    public void testLoadConstant() {
        Status status = OMNI_STATUS_NORMAL;
        assertEquals(status.getValue(), 0);
        status = OMNI_STATUS_ERROR;
        assertEquals(status.getValue(), -1);
        status = OMNI_STATUS_FINISHED;
        assertEquals(status.getValue(), 1);

        AggType aggType = OMNI_AGGREGATION_TYPE_SUM;
        assertEquals(aggType.getValue(), 0);
    }

    /**
     * Test equals.
     */
    @Test
    public void testEquals() {
        Status status = OMNI_STATUS_NORMAL;
        assertEquals(status.getValue(), 0);
    }
}
