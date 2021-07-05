import com.google.api.services.dataflow.model.PubSubIODetails;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.storage.NoopPathValidator;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class Example {
    public static void main(String[] args) throws IOException {
        //PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        List<String> input= Files.lines(Paths.get("word.txt"))
                .map(e->e.replaceAll("\\s+", ""))
               // .flatMap(e->Arrays.stream(e.split(" ")))
                .collect(Collectors.toList());
        DataflowPipelineOptions pipelineOptions = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        pipelineOptions.setJobName("JobDataFlowListWord");
        pipelineOptions.setProject("dataflowproject-318904");
        pipelineOptions.setRegion("us-central1");
        pipelineOptions.setGcpTempLocation("gs://bucket-dataflow-project//temp");
        pipelineOptions.setRunner(DataflowRunner.class);
        Pipeline pipeline = Pipeline.create(pipelineOptions);
       //  final List<String> input = Arrays.asList("topic1","topic2", "topic3");
         pipeline.apply(Create.of(input)).apply(TextIO.write().to("gs://bucket-dataflow-project//output").withSuffix(".txt"));
         pipeline.apply(Create.of(input)).apply(PubsubIO.writeStrings().to("projects/dataflowproject-318904/topics/firstTopic"));
        pipeline.run().waitUntilFinish();
    }


}
