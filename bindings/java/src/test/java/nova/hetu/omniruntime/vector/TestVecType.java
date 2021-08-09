package nova.hetu.omniruntime.vector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import nova.hetu.omniruntime.type.BooleanVecType;
import nova.hetu.omniruntime.type.DoubleVecType;
import nova.hetu.omniruntime.type.IntVecType;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.type.Decimal128VecType;
import nova.hetu.omniruntime.type.LongVecType;
import nova.hetu.omniruntime.utils.OmniErrorType;
import nova.hetu.omniruntime.utils.OmniRuntimeException;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * test vec type
 */
public class TestVecType
{
    /**
     * test vec type
     */
    @Test
    public void testVecType()
    {
        VecType type = getVecTypeFromBase("BIGINT");
        assertEquals(type, LongVecType.LONG);
    }

    private VecType getVecTypeFromBase(String base)
    {
        switch (base) {
            case "INT":
            case "DATE":
                return IntVecType.INTEGER;
            case "BIGINT":
                return LongVecType.LONG;
            case "DOUBLE":
                return DoubleVecType.DOUBLE;
            case "BOOLEAN":
                return BooleanVecType.BOOLEAN;
            default:
                throw new OmniRuntimeException(OmniErrorType.OMNI_UNDEFINED, "Not support Type " + base);
        }
    }

    @Test
    public void testSerialization()
            throws JsonProcessingException
    {
        ObjectMapper map = new ObjectMapper();
        List<nova.hetu.omniruntime.type.VecType> types = new ArrayList<>();
        types.add(LongVecType.LONG);
        types.add(new Decimal128VecType(1,2));
        String result = map.writeValueAsString(types);

        System.out.println(result);
    }
}
