package com.wdpr.customnifiprocessor.processors.sqltocsv;

/**
 * Created by solor031 on 3/8/17.
 */



import com.opencsv.CSVWriter;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;


import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.google.common.annotations.VisibleForTesting;

@SupportsBatching

@Tags({"sql", "get", "rdbms", "database", "csv", "select", "relational"})
@CapabilityDescription("Executes a sql select statement and returns a csv file delimited with the delimiter specified. ")

public class ExecuteSqlToCsv extends AbstractProcessor {


    private static final Validator CHAR_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            // Allows special, escaped characters as input, which is then unescaped and converted to a single character.
            // Examples for special characters: \t (or \u0009), \f.
            input = unescapeString(input);

            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .explanation("Only non-null single characters are supported")
                    .valid((input.length() == 1 && input.charAt(0) != 0) )
                    .build();
        }
    };


    static final PropertyDescriptor CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("sqltocsv processor connection name")
            .displayName("JDBC Connection ")
            .description("Specifies the JDBC Connection Pool to use for sqltocsv processor")
            .identifiesControllerService(DBCPService.class)
            .required(true)
            .build();
    @VisibleForTesting
    static final PropertyDescriptor DELIMITER = new PropertyDescriptor.Builder()
            .name("CSV delimiter")
            .description("Delimiter character for CSV records")
            .addValidator(CHAR_VALIDATOR)
            .expressionLanguageSupported(true)
            .defaultValue(",")
            .build();

    static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
            .name("Select Query")
            .description("The select query to execute ")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();


    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The resultset is converted to csv and is routed to this relationship ")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if the query is invalid")
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(CONNECTION_POOL);
        properties.add(DELIMITER);
        properties.add(QUERY);


        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        return rels;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        final long startNanos = System.nanoTime();
        FlowFile flowFile = session.create();
        final ComponentLog logger = getLogger();

        final String selectquery = context.getProperty(QUERY).evaluateAttributeExpressions().getValue().toString();
        final String delimiterstring = context.getProperty(DELIMITER).evaluateAttributeExpressions().getValue();


        //get controllerservice for the database connection
        final DBCPService dbcpService = context.getProperty(CONNECTION_POOL).asControllerService(DBCPService.class);


        try (final Connection con = dbcpService.getConnection()) {
            con.setAutoCommit(false);
            final Statement st = con.createStatement();
            st.setFetchSize(1000);
            final boolean[] hadWarning = {false};

            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    try {
                        OutputStream bufferedout = new BufferedOutputStream(out);
                        OutputStreamWriter osWriter = new OutputStreamWriter(bufferedout );
                        logger.debug("Executing query {}", new Object[]{selectquery});
                        final ResultSet resultSet = st.executeQuery(selectquery);
                        CSVWriter writer = new CSVWriter(osWriter,delimiterstring.charAt(0));
                        writer.writeAll(resultSet,true);
                        osWriter.flush();
                        writer.close();
                        resultSet.close();


                    } catch (final SQLException e) {
                        throw new ProcessException(e);
                    }

                }
            });


            // Determine the database URL
            String url = "jdbc://unknown-host";
            try {
                url = con.getMetaData().getURL();
            } catch (final SQLException sqle) {
            }

            // Emit a Provenance SEND event
            final long transmissionMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().create(flowFile, "Creates from command: " + selectquery);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (SQLException e1) {
            session.transfer(flowFile, REL_FAILURE);
            getLogger().error("Failed to process query", e1);
        }
        session.commit();


    }
    private static String unescapeString(String input) {
        if (input.length() > 1) {
            input = StringEscapeUtils.unescapeJava(input);
        }
        return input;
    }

}