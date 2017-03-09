package JobControl.processors.GetBatchId;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by solor031 on 2/28/17.
 */
public class GetBatchIdTest {


        private TestRunner testRunner;

        @Before
        public void init() {
            testRunner = TestRunners.newTestRunner(GetBatchId.class);
        }

        @Test
        public void testProcessor() {



    }

}
