package com.wdpr.customnifiprocessor.processors.executebteq;

/**
 * Created by solor031 on 3/3/17.
 */



import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import java.util.HashSet;
import java.io.InputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"command", "process", "source", "external", "invoke", "script", "restricted"})
@CapabilityDescription("Runs an operating system command specified by the user and writes the output of that command to a FlowFile. If the command is expected "
        + "to be long-running, the Processor can output the partial data on a specified interval. When this option is used, the output is expected to be in textual "
        + "format, as it typically does not make sense to split binary data on arbitrary time-based intervals.")
@DynamicProperty(name = "An environment variable name", value = "An environment variable value", description = "These environment variables are passed to the process spawned by this Processor")
@Restricted("Provides operator the ability to execute arbitrary code assuming all permissions that NiFi has.")
@WritesAttributes({
        @WritesAttribute(attribute = "command", description = "Executed command"),
        @WritesAttribute(attribute = "command.arguments", description = "Arguments of the command")
})
public class ExecuteBteqProcess extends AbstractProcessor {

    final static String ATTRIBUTE_COMMAND = "command";
    final static String ATTRIBUTE_COMMAND_ARGS = "command.arguments";

    public static final PropertyDescriptor COMMAND = new PropertyDescriptor.Builder()
            .name("Command")
            .description("Specifies the command to be executed; if just the name of an executable is provided, it must be in the user's environment PATH.")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor COMMAND_ARGUMENTS = new PropertyDescriptor.Builder()
            .name("Command Arguments")
            .description("The arguments to supply to the executable delimited by white space. White space can be escaped by enclosing it in double-quotes.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor WORKING_DIR = new PropertyDescriptor.Builder()
            .name("Working Directory")
            .description("The directory to use as the current working directory when executing the command")
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.createDirectoryExistsValidator(false, true))
            .required(false)
            .build();

    public static final PropertyDescriptor BATCH_DURATION = new PropertyDescriptor.Builder()
            .name("Batch Duration")
            .description("If the process is expected to be long-running and produce textual output, a batch duration can be specified so "
                    + "that the output will be captured for this amount of time and a FlowFile will then be sent out with the results "
                    + "and a new FlowFile will be started, rather than waiting for the process to finish before sending out the results")
            .required(false)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor REDIRECT_ERROR_STREAM = new PropertyDescriptor.Builder()
            .name("Redirect Error Stream")
            .description("If true will redirect any error stream output of the process to the output stream. "
                    + "This is particularly helpful for processes which write extensively to the error stream or for troubleshooting.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("false")
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    private static final Validator characterValidator = new StandardValidators.StringLengthValidator(1, 1);

    static final PropertyDescriptor ARG_DELIMITER = new PropertyDescriptor.Builder()
            .name("Argument Delimiter")
            .description("Delimiter to use to separate arguments for a command [default: space]. Must be a single character.")
            .addValidator(Validator.VALID)
            .addValidator(characterValidator)
            .required(true)
            .defaultValue(" ")
            .build();

    static final PropertyDescriptor BTEQ_FILENAME = new PropertyDescriptor.Builder()
            .name("BTEQ file template location")
            .description("Location of the Bteq file template.Access using $BTEQ_FILENAME in shell script")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("${nifi.custom.teradata.bteqfilename}")
            .required(true)
            .build();

    static final PropertyDescriptor BTEQ_RUNFILE = new PropertyDescriptor.Builder()
            .name("BTEQ run file location")
            .description("Location of a temporary file into which the bteq script for execution is written into")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .defaultValue(" ")
            .build();

    static final PropertyDescriptor LOGIN_INFO = new PropertyDescriptor.Builder()
            .name("Login information")
            .description("Login information to execute bteq script in the format 'servername/uid,pwd'")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("${nifi.custom.teradata.logininfo}")
            .required(true)
            .build();

    static final PropertyDescriptor DB_REFERENCE = new PropertyDescriptor.Builder()
            .name("Database reference names ")
            .description("Database reference names seperated by ';' eg:-'WORK_DB:SB_DMI_DB;TARGET_DB:SB_DMI_DB'")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("${nifi.custom.teradata.dbreference}")
            .required(true)
            .build();



    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All created FlowFiles are routed to this relationship")
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original parameter file fro incoming connection")
            .description("All original parameter files from the incoming connection routed to this relationship")
            .build();

    private volatile Process externalProcess;

    private volatile ExecutorService executor;
    private Future<?> longRunningProcess;
    private AtomicBoolean failure = new AtomicBoolean(false);
    private volatile ProxyOutputStream proxyOut;

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_ORIGINAL);
        return rels;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(COMMAND);
        properties.add(COMMAND_ARGUMENTS);
        properties.add(BATCH_DURATION);
        properties.add(REDIRECT_ERROR_STREAM);
        properties.add(ARG_DELIMITER);
        properties.add(BTEQ_FILENAME);
        properties.add(BTEQ_RUNFILE);
        properties.add(DB_REFERENCE);
        properties.add(LOGIN_INFO);
        return properties;
    }


    //to get dynamic property values .
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("Sets the environment variable '" + propertyDescriptorName + "' for the process' environment")
                .dynamic(true)
                .expressionLanguageSupported(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();
    }



    @OnScheduled
    public void setupExecutor(final ProcessContext context) {
        executor = Executors.newFixedThreadPool(context.getMaxConcurrentTasks() * 2, new ThreadFactory() {
            private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();

            @Override
            public Thread newThread(final Runnable r) {
                final Thread t = defaultFactory.newThread(r);
                t.setName("ExecuteProcess " + getIdentifier() + " Task");
                return t;
            }
        });
    }

    @OnUnscheduled
    public void shutdownExecutor() {
        try {
            executor.shutdown();
        } finally {
            if (this.externalProcess.isAlive()) {
                this.getLogger().info("Process hasn't terminated, forcing the interrupt");
                this.externalProcess.destroyForcibly();
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        FlowFile fileToProcess = null;
        if(context.hasIncomingConnection())
        {
            fileToProcess = session.get();
        }

        // If we have no FlowFile, and all incoming connections are self-loops then we can continue on. 
        // However, if we have no FlowFile and we have connections coming from other Processors, then 
        // we know that we should run only if we have a FlowFile.

        if(fileToProcess== null && context.hasNonLoopConnection()){
            return;
        }

        //getting the properties from the incoming flowfile.
        Map<PropertyDescriptor , String> processorProperties = context.getProperties();
        Map<String,String> generatedAttributes = new HashMap<String,String>();
        generatedAttributes = fileToProcess.getAttributes();


        session.transfer(fileToProcess ,REL_ORIGINAL);

        if (proxyOut==null) {
            proxyOut = new ProxyOutputStream(getLogger());
        }


        final Long batchNanos = context.getProperty(BATCH_DURATION).asTimePeriod(TimeUnit.NANOSECONDS);

        final String command = context.getProperty(COMMAND).getValue();
        final String arguments = context.getProperty(COMMAND_ARGUMENTS).isSet()
                ? context.getProperty(COMMAND_ARGUMENTS).evaluateAttributeExpressions(fileToProcess).getValue()
                : null;

        //making  the attributes  available in the environment
        final Map<String, String> environment = new HashMap<>();
        environment.put("BTEQ_FILENAME",context.getProperty(BTEQ_FILENAME).evaluateAttributeExpressions().getValue());
        environment.put("BTEQ_RUNFILE",context.getProperty(BTEQ_RUNFILE).evaluateAttributeExpressions(fileToProcess).getValue());
        environment.put("DB_REFERENCE",context.getProperty(DB_REFERENCE).evaluateAttributeExpressions(fileToProcess).getValue());
        environment.put("LOGIN_INFO",context.getProperty(LOGIN_INFO).evaluateAttributeExpressions().getValue());
        //if values are coming from a customproperties file then refer using .evaluateAttributeExpressions().getValue()
        //if values are coming from the incoming flowfile attribute then refer to it using
        //context.getProperty(<attributename>).evaluateAttributeExpressions(flowFile)
        // environment.put("LOGIN_INFO",context.getProperty(LOGIN_INFO).evaluateAttributeExpressions().getValue());
        //making the dynamic attributes available in the environment


        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (entry.getKey().isDynamic()) {
                environment.put(entry.getKey().getName(), context.getProperty(entry.getKey()).evaluateAttributeExpressions(fileToProcess).getValue());
            }
        }



        final List<String> commandStrings = createCommandStrings(context, command, arguments);
        final String commandString = StringUtils.join(commandStrings, " ");

        if (longRunningProcess == null || longRunningProcess.isDone()) {
            try {
                longRunningProcess = launchProcess(context, commandStrings,environment, batchNanos, proxyOut);
            } catch (final IOException ioe) {
                getLogger().error("Failed to create process due to {}", new Object[] { ioe });
                context.yield();
                return;
            }
        } else {
            getLogger().info("Read from long running process");
        }

        if (!isScheduled()) {
            getLogger().info("User stopped processor; will terminate process immediately");
            longRunningProcess.cancel(true);
            return;
        }

        // Create a FlowFile that we can write to and set the OutputStream for the FlowFile
        // as the delegate for the ProxyOuptutStream, then wait until the process finishes
        // or until the specified amount of time
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(final OutputStream flowFileOut) throws IOException {
                try (final OutputStream out = new BufferedOutputStream(flowFileOut)) {
                    proxyOut.setDelegate(out);

                    if (batchNanos == null) {
                        // we are not creating batches; wait until process terminates.
                        // NB!!! Maybe get(long timeout, TimeUnit unit) should
                        // be used to avoid waiting forever.
                        try {
                            longRunningProcess.get();
                        } catch (final InterruptedException ie) {
                        } catch (final ExecutionException ee) {
                            getLogger().error("Process execution failed due to {}", new Object[] { ee.getCause() });
                        }
                    } else {
                        // wait the allotted amount of time.
                        try {
                            TimeUnit.NANOSECONDS.sleep(batchNanos);
                        } catch (final InterruptedException ie) {
                        }
                    }

                    proxyOut.setDelegate(null); // prevent from writing to this
                    // stream
                }
            }
        });

        if (flowFile.getSize() == 0L) {
            // If no data was written to the file, remove it
            session.remove(flowFile);
        } else if (failure.get()) {
            // If there was a failure processing the output of the Process, remove the FlowFile
            session.remove(flowFile);
            getLogger().error("Failed to read data from Process, so will not generate FlowFile");
        } else {
            // add command and arguments as attribute
            flowFile = session.putAllAttributes(flowFile,generatedAttributes);
            flowFile = session.putAttribute(flowFile, ATTRIBUTE_COMMAND, command);
            if(arguments != null) {
                flowFile = session.putAttribute(flowFile, ATTRIBUTE_COMMAND_ARGS, arguments);
            }

            // All was good. Generate event and transfer FlowFile.
            session.getProvenanceReporter().create(flowFile, "Created from command: " + commandString);
            getLogger().info("Created {} and routed to success", new Object[] { flowFile });
            session.transfer(flowFile, REL_SUCCESS);
        }

        // Commit the session so that the FlowFile is transferred to the next processor
        session.commit();
    }

    protected List<String> createCommandStrings(final ProcessContext context, final String command, final String arguments) {
        final List<String> args = ArgumentUtils.splitArgs(arguments, context.getProperty(ARG_DELIMITER).getValue().charAt(0));
        final List<String> commandStrings = new ArrayList<>(args.size() + 1);
        commandStrings.add(command);
        commandStrings.addAll(args);
        return commandStrings;
    }

    protected Future<?> launchProcess(final ProcessContext context, final List<String> commandStrings, final Map<String, String> environment,final Long batchNanos,
                                      final ProxyOutputStream proxyOut) throws IOException {

        final Boolean redirectErrorStream = context.getProperty(REDIRECT_ERROR_STREAM).asBoolean();

        final ProcessBuilder builder = new ProcessBuilder(commandStrings);
        final String workingDirName = context.getProperty(WORKING_DIR).getValue();
        if (workingDirName != null) {
            builder.directory(new File(workingDirName));
        }
//adding dynamic attributes to the environment
      /*  final Map<String, String> environment = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (entry.getKey().isDynamic()) {
                environment.put(entry.getKey().getName(), entry.getValue());
            }
        }*/

        if (!environment.isEmpty()) {
            builder.environment().putAll(environment);
        }

        getLogger().info("Start creating new Process > {} ", new Object[] { commandStrings });
        this.externalProcess = builder.redirectErrorStream(redirectErrorStream).start();

        // Submit task to read error stream from process
        if (!redirectErrorStream) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try (final BufferedReader reader = new BufferedReader(new InputStreamReader(externalProcess.getErrorStream()))) {
                        reader.lines().filter(line -> line != null && line.length() > 0).forEach(getLogger()::warn);
                    } catch (final IOException ioe) {
                    }
                }
            });
        }

        // Submit task to read output of Process and write to FlowFile.
        failure = new AtomicBoolean(false);
        final Future<?> future = executor.submit(new Callable<Object>() {
            @Override
            public Object call() throws IOException {
                try {
                    if (batchNanos == null) {
                        // if we aren't batching, just copy the stream from the
                        // process to the flowfile.
                        try (final BufferedInputStream bufferedIn = new BufferedInputStream(externalProcess.getInputStream())) {
                            final byte[] buffer = new byte[4096];
                            int len;
                            while ((len = bufferedIn.read(buffer)) > 0) {

                                // NB!!!! Maybe all data should be read from
                                // input stream in case of !isScheduled() to
                                // avoid subprocess deadlock?
                                // (we just don't write data to proxyOut)
                                // Or because we don't use this subprocess
                                // anymore anyway, we don't care?
                                if (!isScheduled()) {
                                    return null;
                                }

                                proxyOut.write(buffer, 0, len);
                            }
                        }
                    } else {
                        // we are batching, which means that the output of the
                        // process is text. It doesn't make sense to grab
                        // arbitrary batches of bytes from some process and send
                        // it along as a piece of data, so we assume that
                        // setting a batch during means text.
                        // Also, we don't want that text to get split up in the
                        // middle of a line, so we use BufferedReader
                        // to read lines of text and write them as lines of text.
                        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(externalProcess.getInputStream()))) {
                            String line;

                            while ((line = reader.readLine()) != null) {
                                if (!isScheduled()) {
                                    return null;
                                }

                                proxyOut.write((line + "\n").getBytes(StandardCharsets.UTF_8));
                            }
                        }
                    }
                } catch (final IOException ioe) {
                    failure.set(true);
                    throw ioe;
                } finally {
                    try {
                        // Since we are going to exit anyway, one sec gives it an extra chance to exit gracefully.
                        // In the future consider exposing it via configuration.
                        boolean terminated = externalProcess.waitFor(1000, TimeUnit.MILLISECONDS);
                        int exitCode = terminated ? externalProcess.exitValue() : -9999;
                        getLogger().info("Process finished with exit code {} ", new Object[] { exitCode });
                    } catch (InterruptedException e1) {
                        Thread.currentThread().interrupt();
                    }
                }

                return null;
            }
        });

        return future;
    }


    /**
     * Output stream that is used to wrap another output stream in a way that the underlying output stream can be swapped out for a different one when needed
     */
    private static class ProxyOutputStream extends OutputStream {

        private final ComponentLog logger;

        private final Lock lock = new ReentrantLock();
        private OutputStream delegate;

        public ProxyOutputStream(final ComponentLog logger) {
            this.logger = logger;
        }

        public void setDelegate(final OutputStream delegate) {
            lock.lock();
            try {
                logger.trace("Switching delegate from {} to {}", new Object[]{this.delegate, delegate});
                this.delegate = delegate;
            } finally {
                lock.unlock();
            }
        }

        private void sleep(final long millis) {
            try {
                Thread.sleep(millis);
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void write(final int b) throws IOException {
            lock.lock();
            try {
                while (true) {
                    if (delegate != null) {
                        logger.trace("Writing to {}", new Object[]{delegate});

                        delegate.write(b);
                        return;
                    } else {
                        lock.unlock();
                        sleep(1L);
                        lock.lock();
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void write(final byte[] b, final int off, final int len) throws IOException {
            lock.lock();
            try {
                while (true) {
                    if (delegate != null) {
                        logger.trace("Writing to {}", new Object[]{delegate});
                        delegate.write(b, off, len);
                        return;
                    } else {
                        lock.unlock();
                        sleep(1L);
                        lock.lock();
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void write(final byte[] b) throws IOException {
            write(b, 0, b.length);
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public void flush() throws IOException {
            lock.lock();
            try {
                while (true) {
                    if (delegate != null) {
                        delegate.flush();
                        return;
                    } else {
                        lock.unlock();
                        sleep(1L);
                        lock.lock();
                    }
                }
            } finally {
                lock.unlock();
            }
        }
    }
}