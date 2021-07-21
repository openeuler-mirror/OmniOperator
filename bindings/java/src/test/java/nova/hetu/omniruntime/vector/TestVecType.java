package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.constants.VecType;
import nova.hetu.omniruntime.utils.OmniErrorType;
import nova.hetu.omniruntime.utils.OmniRuntimeException;
import org.testng.annotations.Test;

import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_BOOLEAN;
import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_DOUBLE;
import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_INT;
import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_LONG;
import static org.testng.Assert.assertEquals;

/**
 * test vec type
 */
public class TestVecType {
    /**
     * test vec type
     */
    @Test
    public void testVecType() {
        assertEquals(OMNI_VEC_TYPE_LONG.getValue(), 2);
        VecType type = getVecTypeFromBase("BIGINT");
        assertEquals(type, OMNI_VEC_TYPE_LONG);
    }

    private VecType getVecTypeFromBase(String base) {
        switch (base) {
            case "INT":
            case "DATE":
                return OMNI_VEC_TYPE_INT;
            case "BIGINT":
                return OMNI_VEC_TYPE_LONG;
            case "DOUBLE":
                return OMNI_VEC_TYPE_DOUBLE;
            case "BOOLEAN":
                return OMNI_VEC_TYPE_BOOLEAN;
            default:
                throw new OmniRuntimeException(OmniErrorType.OMNI_UNDEFINED, "Not support Type " + base);
        }
    }
}
