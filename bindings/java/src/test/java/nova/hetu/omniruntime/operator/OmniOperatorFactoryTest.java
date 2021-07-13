package nova.hetu.omniruntime.operator;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The type Omni operator factory test.
 */
public class OmniOperatorFactoryTest {
    /**
     * The type Mock operator factory.
     */
    public static class MockOperatorFactory extends OmniOperatorFactory<MockOperatorFactory.MockContext> {
        /**
         * Instantiates a new Mock operator factory.
         *
         * @param context the context
         */
        public MockOperatorFactory(long context) {
            super(new MockContext(context));
        }

        @Override
        protected long createNativeOperatorFactory(MockContext context) {
            return System.nanoTime();
        }

        /**
         * The type Mock context.
         */
        public static class MockContext extends OmniOperatorFactoryContext {
            private final long context;

            /**
             * Instantiates a new Mock context.
             *
             * @param context the context
             */
            public MockContext(long context) {
                this.context = context;
            }

            @Override
            public int hashCode() {
                return Objects.hash(context);
            }

            @Override
            public boolean equals(Object that) {
                return context == ((MockContext) that).context;
            }
        }
    }

    /**
     * Test operator factory cache.
     */
    @Test
    public void testOperatorFactoryCache() {
        MockOperatorFactory factory1 = new MockOperatorFactory(1);
        MockOperatorFactory factory2 = new MockOperatorFactory(1);
        assertEquals(factory1.getNativeOperatorFactory(), factory2.getNativeOperatorFactory());
        MockOperatorFactory factory3 = new MockOperatorFactory(2);
        assertNotEquals(factory1.getNativeOperatorFactory(), factory3.getNativeOperatorFactory());
    }

    /**
     * The Check factory.
     */
    MockOperatorFactory checkFactory = new MockOperatorFactory(1);

    /**
     * Test operator factory cache multi thread.
     *
     * @throws InterruptedException the interrupted exception
     */
    @Test
    public void testOperatorFactoryCacheMultiThread() throws InterruptedException {
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            threads.add(new Thread(() -> {
                MockOperatorFactory factory = new MockOperatorFactory(1);
                assertEquals(checkFactory.getNativeOperatorFactory(), factory.getNativeOperatorFactory());
            }));
        }
        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
    }
}
