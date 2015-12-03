import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

public class MyYarnClient {
    public static final String AMOUNT_OF_LINES_IN_INPUT_FILES_COUNTER = "amountOfLinesInInputFiles";
    public static final String LINES_AMOUNTS_MAP = "./lines_amount_map.csv";

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException, YarnException {
        //Creating a JobConf object and assigning a job name for identification purposes
        JobConf conf = new JobConf(MyYarnClient.class);
        conf.setJobName("LinesAmount");

        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();

        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

        // set the application submission context
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();

        appContext.setKeepContainersAcrossApplicationAttempts(false);
        appContext.setApplicationName("bla-bla");

        // Set up the container launch context for the application master
        Map<String, LocalResource> localResources = null;
        Map<String, String> env = null;
        List<String> commands = null;
        ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
                localResources, env, commands, null, null, null);
        Resource capability = Resource.newInstance(200, 1);
        appContext.setResource(capability);
        appContext.setAMContainerSpec(amContainer);

        Priority pri = Priority.newInstance(0);
        appContext.setPriority(pri);

        yarnClient.submitApplication(appContext);

    }
}
