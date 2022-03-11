package com.nttdata.gcp;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class PubSubToBigQuery {
    /*
     * The logger to output status messages to.
     */
    private static final Logger log = LogManager.getLogger(PubSubToBigQuery.class);

    static final TupleTag<Account> parsedMessages = new TupleTag<Account>(){};
    static final TupleTag<String> unparsedMessages = new TupleTag<String>(){};

    /**
     * The {@link Options} class provides the custom execution options passed by
     * the executor at the command-line
     */
    public interface Options extends PipelineOptions, StreamingOptions {
        @Description("Input topic name")
        String getInputTopic();

        void setInputTopic(String inputTopic);

        @Description("BigQuery table name")
        String getOutputTable();

        void setOutputTable(String outputTableName);

        @Description("Output topic name")
        String getOutputTopic();

        void setOutputTopic(String outputTopic);

        @Description("This determines whether the template reads from "+"a pub/sub subcription or a topic")
        @Default.Boolean(false)
        Boolean getUseSubscription();

        void setUseSubscription(Boolean value);

        @Description("The Cloud Pub/Sub subscriptio to consume from "
                + "The name should be in the format of "
                + "projects/<project-id>/subscriptions/<subscription-name>.")
        String getInputSubscription();

        void setInputSubscription(String value);

    }
    /**
     * The main entry-point for pipeline execution. This method will start the
     * pipeline but will not wait for it's execution to finish. If blocking
     * execution is required, use the {@link PubSubToBigQuery#run(Options)} method
     * to start the pipeline and invoke {@code result.waitUntilFinish()} on the
     * {@link PipelineResult}.
     *
     * @param args The command-line args passed by the executor.
     */
    public static void main(String[] args) {
        PipelineOptionsFactory.register(Options.class);
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        run(options);
    }

    /**
     * A class used for parsing JSON web server events Annotated with @DefaultSchema
     * to allow the use of Beam schemas and <Row> object
     */
    @DefaultSchema(JavaFieldSchema.class)
    public static class Account implements Serializable {
        int id;
        String name;
        String surname;

    /*
     * Constructor
     */
    public Account(int id, String name, String surname){
        this.id = id;
        this.name = name;
        this.surname = surname;
    }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getSurname() {
            return surname;
        }

        public void setSurname(String surname) {
            this.surname = surname;
        }

        @Override
        public String toString() {
            return "{" + "\"id=\"=" + id + ", \"name\"=\"" + name + '\"' + ", \"surname\"=\"" + surname + '\"' + '}';
        }
    }
    /**
     * A PTransform accepting Json and outputting tagged CommonLog with Beam Schema
     * or raw Json string if parsing fails
     */
    public static class PubsubMessageToAccount extends PTransform<PCollection<String>, PCollectionTuple>{
        @Override
        public PCollectionTuple expand(PCollection<String> input){
            return input.apply("JsonToAccount", ParDo.of(new DoFn<String, Account>() {
                @ProcessElement
                public void processElement(ProcessContext context){
                    String json = context.element();
                    Gson gson = new Gson();
                    try {
                        Account account = gson.fromJson(json, Account.class);
                        context.output(parsedMessages, account);
                        System.out.println("parsedMessages:"+account.toString());
                    } catch (JsonSyntaxException e){
                        context.output(unparsedMessages, json);
                        System.out.println("unparsedMessages:"+json);
                    }
                }
            }).withOutputTags(parsedMessages, TupleTagList.of(unparsedMessages)));
        }
    }

    /**
     * Runs the pipeline to completion with the specified options. This method does not wait until the pipeline
     * is finished before returning. Invoke {@code result.waitUntilFinish()} on the result object to block
     * until the pipeline is finished running if blocking programmatic execution is required.
     * @param options the execution options
     * @return the pipeline result
     */
    public static PipelineResult run(Options options){
        // Create pipeline
        Pipeline pipeline = Pipeline.create(options);
        /*
         * Steps: 1) Read messages from pub/sub IO 2) Transform something 3) Write
         * something into BigQuery and DLQ topic
         */
        log.info("Building pipeline...");
//        String inputTopic = "projects/nttdata-c4e-bde/topics/uc1-input-topic-2";
//        String outputTopic = "projects/nttdata-c4e-bde/topics/uc1-dlq-topic-2";
//        String outputTable = "nttdata-c4e-bde:uc1_2.account";
        try {
            // Build the table schema for output table
            List<TableFieldSchema> fields = new ArrayList<>();
            fields.add(new TableFieldSchema().setName("id").setType("INTEGER"));
            fields.add(new TableFieldSchema().setName("name").setType("STRING"));
            fields.add(new TableFieldSchema().setName("surname").setType("STRING"));
            TableSchema schema = new TableSchema().setFields(fields);

            PCollection<String> messages = null;
            if(options.getUseSubscription()){
                messages = pipeline.apply("ReadPubSubSubscription",
                        PubsubIO.readStrings().fromSubscription(options.getInputSubscription()));
            }
            else {
                messages = pipeline.apply("ReadPubSubTopic",
                        PubsubIO.readStrings().fromTopic(options.getInputTopic()));
            }
//            PCollectionTuple transformOut =
//                    pipeline.apply("ReadPubSubMessages", PubsubIO.readStrings()
//                            .fromSubscription(options.getInputTopic())).apply("ConvertMessageToAccount", new PubsubMessageToAccount())
            PCollectionTuple transformOut =
                    messages.apply("ConvertMessageToAccount", new PubsubMessageToAccount());

            // Write parsed messages to BigQuery
            transformOut.get(parsedMessages).apply("ToBQRow",ParDo.of(new DoFn<Account, TableRow>()
            {
                @ProcessElement
                public void processElement(ProcessContext c) throws Exception {
                    TableRow row = new TableRow();
                    Account info = c.element();
                    row.set("id",info.getId());
                    row.set("name",info.getName());
                    row.set("surname",info.getSurname());
                    c.output(row);
                }
            })).apply(BigQueryIO.writeTableRows().to(options.getOutputTable())
                    .withSchema(schema)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
            // Write malformed data to dlq topic
            transformOut.get(unparsedMessages).apply("Write to PubSub",
                    PubsubIO.writeStrings().to(options.getOutputTopic()));
        } catch (Exception e){
            log.error(e.getMessage());
        }
        return pipeline.run();
    }
}
