package com.wdpr.customnifiprocessor.processors.executebteq;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by solor031 on 3/3/17.
 */

public class ExecuteBteqProcessTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(ExecuteBteqProcess.class);
    }

    @Test
    public void testProcessor() {

    }

}
