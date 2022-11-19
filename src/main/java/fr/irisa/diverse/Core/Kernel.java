package fr.irisa.diverse.Core;

import fr.irisa.diverse.Flow.Node;
import fr.irisa.diverse.Jupyter.JupyterChannels.*;
import fr.irisa.diverse.Jupyter.JupyterMessaging.Manager;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The kernel is the element that runs the python code and decide when it is needed to inform every interface
 * of the executed code, the result, that it is busy, idle, etc.
 *
 * 5 channels are used to communicate with the fr.irisa.diverse.Core.Kernel
 * (see fr.irisa.diverse.Jupyter.JupyterMessaging in Jupyter doc to know more about it).
 *
 * The kernel is embedded into a Docker container, which creates a connexion_file that this class will read
 * to know on which ports to connect the sockets.
 *
 * IMPORTANT : 1 kernel per node, 1 node per kernel. It is a 1-1 relation.
 *
 * Created by antoine on 28/04/17.
 */
public class Kernel {

    // Process and Docker container id
    private Process container;
    private String containerId = null;

    // Channels
    public ShellChannel shell = null;
    public IOPubChannel iopub = null;
    public StdinChannel stdin = null;
    public HeartbeatChannel hb = null;
    public ShellChannel control = null;

    // Kernel state & execution info
    private boolean idle = false;
    private long nbExecutions = 0;

    // Messages info
    private String session = null;
    private String identity = null; // uuid of the messages exchanged on the channels
    private Manager messagesManager = null;

    // Channels' connexion infos
    private String transport = null;
    private String ip = null;
    private String signature_scheme = null;
    private String key = null;
    private String pathToConnexionFilesFolder = null;
    private String pathToWorkspaceStorage = null;
    private String pathToUtils = null;
    private JSONParser parser = null;

    // Workspace related info
    public String linkedNodeId;
    public Workspace owningWorkspace;


    /*==================================================================================================================
                                                    CONSTRUCTOR
     =================================================================================================================*/

    public Kernel (String linkedNodeId, Workspace workspace) {
        // Instantiate objects that will be useful later
        this.parser = new JSONParser();

        // Set linkedNodeId & workspace
        this.linkedNodeId = linkedNodeId;
        this.owningWorkspace = workspace;

        // Set the path to the workspace
        this.pathToWorkspaceStorage = Root.PATH_TO_PROJECT_STORAGE + "/workspaces/" + owningWorkspace.getUuid();
        this.pathToUtils = Root.PATH_TO_PROJECT_STORAGE + "/workspaces/utils";

        // Retrieve the absolute path to resources/connexion_files
        pathToConnexionFilesFolder = Root.PATH_TO_PROJECT_STORAGE + "/connexion_files";

        try {
            // Start a new container from image antoinecheronirisa/lmt-python-core
            startContainer();

            // Retrieve the file containing the connection_info from the kernel. These information are used to connect
            // to the proper zmq sockets
            if(this.containerId != null) {
                // Path to the connection_info file
                String absolutePathToConnexionInfoFile = pathToConnexionFilesFolder + "/" + containerId + ".json";

                createChannelsFromConnexionFile(absolutePathToConnexionInfoFile);

                startChannels();
                idle = true;
                shell.sendKernelInfoRequest();

                // Create a message manager that will handle reaction to incoming messages
                messagesManager = new Manager(this);
            }

        } catch (FailedKernelStartException | FailedRetrievingContainerIPException | IOException | ParseException | InterruptedException e) {
            e.printStackTrace();
        }

    }

    /* =================================================================================================================
                                                        PUBLIC METHODS
       ===============================================================================================================*/

    /**
     * Stop the kernel and its linked Docker container.
     */
    public void stop () {
        stopChannels();
        stopContainer();
        deleteConnexionFile();
    }

    /**
     * Stop the execution of kernel, not the container
     */
    public void stopExecution () {
        // TODO
    }

    /**
     * Verify that we are still connected to the 5 channels. If not restart them.
     * TODO : the restart feature
     */
    public void verifyChannelsAreOk () {
        verifyChannelIsOk(shell);
        verifyChannelIsOk(iopub);
        verifyChannelIsOk(hb);
        verifyChannelIsOk(stdin);
        verifyChannelIsOk(control);
    }

    /**
     * Do what's needed when receiving the result of the execution of a code.
     * It verify that we were waiting for this response and store the result into the node.
     *
     * @param result : the result received from the channel's stdout
     */
    public void handleExecutionResult (String[] result) {
        // Build an object containing the results associated with their variable
        JSONParser parser = new JSONParser();
        JSONObject res = new JSONObject();
        JSONObject pickled = new JSONObject();
        if (result.length > 1) {
            // First skip the line until we meet the beginning line that is delimited by the identifier :
            // #BEGINNING OF DATA RETRIEVING
            // To change it, go into ressources/workspaces/utils/sendTheseDataToNextNodes
            int beginningLineIndex = 0;
            while (beginningLineIndex < result.length && !result[beginningLineIndex].equals("#BEGINNING OF DATA RETRIEVING")) {
                beginningLineIndex++;
            }

            if (beginningLineIndex < result.length) {
                // Store each key, value pair after the delimiter
                // A typical line is :
                //  key theKey
                //  pickle thePickledValue
                //  json theJsonifiedValue
                //  their might be several lines to display a value, so we have to loop over the lines.
                int i = beginningLineIndex+1;
                while(i<result.length) {
                    // Retrieve the key
                    int firstIndexOfKey = 4;
                    while (result[i].charAt(firstIndexOfKey) == ' ') { firstIndexOfKey++; }
                    String key = result[i].substring(firstIndexOfKey);
                    i++;

                    // Retrieve the pickled value
                    int firstIndexOfPickledValue = 6;
                    while (result[i].charAt(firstIndexOfPickledValue) == ' ') { firstIndexOfPickledValue++; }
                    String pickle = result[i].substring(firstIndexOfPickledValue);
                    i++;

                    // Retrieve the jsonified value
                    int firstIndexOfJSON = 5;
                    while (result[i].charAt(firstIndexOfJSON) == ' ') { firstIndexOfJSON++; }
                    // Search for next occurence of key
                    int lastLineOfJSON = i;
                    while (lastLineOfJSON+1 < result.length && result[lastLineOfJSON+1].length() >= 3 && !result[lastLineOfJSON+1].substring(0,3).equals("key")) { lastLineOfJSON++; }
                    // Add each line of JSON data into a String.
                    String JSON = result[i].substring(firstIndexOfJSON);
                    i++;
                    while (i < result.length && i <= lastLineOfJSON) {
                        i++;
                        JSON += result[i];
                    }

                    // Jsonify the read line.
                    Object value = new Object();
                    try {
                        value = parser.parse(JSON);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }

                    // Store the JSON and the pickle in the res object
                    res.put(key, value);
                    pickled.put(key, pickle);
            }
            }
        } else {
            // TODO : send an error
        }

        // Tell the node to store the result
        Node linkedNode = owningWorkspace.getFlow().getNode(linkedNodeId, owningWorkspace.getUuid());
        linkedNode.setJsonResult(res);
        linkedNode.setPickledResult(pickled);


    }

    /* =================================================================================================================
                                           PUBLIC FUNCTIONS TO INTERACT WITH KERNEL
       ===============================================================================================================*/

    /**
     * Require the kernel to execute a code on the Jupyter Kernel.
     *
     * @param code : the code to execute
     */
    public void executeCode (String code, Node node) {
        // Add a few lines on top of the code to import the sendTheseDataToNextNodes function
        String codeToExecute = "import sys\nimport os\nimport pickle\nimport json\nsys.path.append('/home/diverse/workspace')\n" +
                "sys.path.append('/home/diverse/utils')\nfrom sendTheseDataToNextNodes import sendTheseDataToNextNodes\n\n";

        // Add all the imports the user wrote
        int lastImportIndex = code.lastIndexOf("import");
        int indexForVarInjection = 0;
        if (lastImportIndex != -1) {
            indexForVarInjection = lastImportIndex + code.substring(lastImportIndex).indexOf("\n") + 1;
            codeToExecute += code.substring(0, indexForVarInjection) + "\n";
        }

        // Add the variables retrieved from the previous nodes
        JSONObject var = node.getPreviousNodesData();
        JSONObject jsonified = (JSONObject) var.get("jsonified");
        JSONObject pickled = (JSONObject) var.get("pickled");

        // Add all pickled variables
        Set pkeys = pickled.keySet();
        Iterator<String> pkeysIterator = pkeys.iterator();
        while(pkeysIterator.hasNext()) {
            String key = pkeysIterator.next();
            codeToExecute += key + " = pickle.loads(" + pickled.get(key).toString() + ")\n";
        }

        // Add all jsonified variables
        Set jkeys = jsonified.keySet();
        Iterator<String> jkeysIterator = jkeys.iterator();
        while(jkeysIterator.hasNext()) {
            String key = jkeysIterator.next();
            codeToExecute += key + " = json.loads('" + jsonified.get(key).toString() + "')\n";
        }

        // Add the rest of the code the user typed
        codeToExecute += "\n" + code.substring(indexForVarInjection);

        System.out.println("Executing code");

        // Send the execution request message on the shell
        messagesManager.sendMessageOnShell().sendExecuteRequestMessage(codeToExecute);
    }

    /* =================================================================================================================
                                                    PRIVATE FUNCTIONS
       ===============================================================================================================*/

    /**
     * Start the container. Must be called only while creating a new instance of Kernel.
     *
     * @throws FailedKernelStartException : exception telling that the kernel failed starting.
     */
    private void startContainer() throws FailedKernelStartException {

        File script = null;

        try {
            // Write the bash script that will start the container
            script = Files.createTempFile("script", null).toFile();
            Writer streamWriter = new OutputStreamWriter(new FileOutputStream(script));
            PrintWriter printWriter = new PrintWriter(streamWriter);
            printWriter.println("#!/bin/bash");
            printWriter.println("docker run -d -v " + pathToConnexionFilesFolder + ":/home/diverse/connexion_files -v " + pathToWorkspaceStorage + ":/home/diverse/workspace -v " + pathToUtils + ":/home/diverse/utils antoinecheronirisa/lmt-python-core");
            printWriter.println("exit");
            printWriter.close();

            // Create the object that let us start run the command
            ProcessBuilder pb = new ProcessBuilder("bash", script.toString());

            // Start container
            this.container = pb.start();

            // Retrieve the outpustream to read the container id

            BufferedReader in = new BufferedReader(new InputStreamReader(this.container.getInputStream()));

            // The outputstream is only one line long and contains the newly created container's id
            this.containerId = in.readLine().substring(0,12);

            // Log the containerId of the newly started docker Jupyter
            System.out.println("Kernel started with id " + containerId);
            String line = null;
            while((line=in.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException e) {
            throw new FailedKernelStartException();
        } finally {
            // Everything went well
            if (script != null) script.delete();
        }

    }

    /**
     * Stop the container linked to this kernel.
     * Usually called when stopping the whole server.
     */
    private void stopContainer () {
        // Run a script to stop the running container
        File script;

        try {
            // Write the bash script that will stop the running container
            script = Files.createTempFile("script-stop", null).toFile();
            Writer streamWriter = new OutputStreamWriter(new FileOutputStream(script));
            PrintWriter printWriter = new PrintWriter(streamWriter);
            printWriter.println("#!/bin/bash");
            printWriter.println("docker stop " + containerId);
            printWriter.println("docker rm" + containerId);
            printWriter.println("exit");
            printWriter.close();

            // Create the object that let us run the command
            ProcessBuilder pb = new ProcessBuilder("bash", script.toString());

            System.out.println("Stopping container " + containerId + "...");
            // Run the command that stop the container
            this.container = pb.start();

            // When stopping a container, docker output the container id after stopping it.
            // To make sure that the container stop before the program exit, we read the output stream in order
            // to make this program wait until the container stops.
            BufferedReader in = new BufferedReader(new InputStreamReader(this.container.getInputStream()));
            in.readLine();
            System.out.println("\033[32m" + "[INFO]" + "\033[0m" + " Container " + containerId + " successfully stopped");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // Everything went well
            container.destroy();
        }
    }

    /**
     * Start the 5 ZMQ channels that communicate with the Jupyter kernel that is inside a Docker container.
     */
    private void startChannels () {
        shell.start();
        iopub.start();
        stdin.start();
        hb.start();
        control.start();
    }

    /**
     * Stop and close the 5 ZMQ channels and their Thread.
     */
    private void stopChannels() {
        // Fix a bug that prevent from stopping
        shell.doLog(false);
        iopub.doLog(false);
        stdin.doLog(false);
        hb.doLog(false);
        control.doLog(false);

        try {
            shell.stop();
            iopub.stop();
            stdin.stop();
            hb.stop();
            control.stop();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    /**
     * Verify that a channel is running and connected
     *
     * @param channel : the channel to check
     */
    private void verifyChannelIsOk(JupyterChannel channel) {
        if (!channel.isRunning()) {
            try {
                channel.stop();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            channel.start();
        }
    }

    /**
     * Create the ZMQ sockets and configure their properties and endpoints from a connexion_file.
     * This connexion_file is generated by the code of the Jupyter kernel inside the Docker container.
     *
     * The name of the connexion_file is {{containerId}}.json
     *
     * @param path : the absolute Path to the connexion_file
     * @throws InterruptedException : Exception from Thread, can be thrown if interrupted while sleeping.
     * @throws FailedRetrievingContainerIPException : Thrown if impossible to retrieve the container's IP
     * @throws ParseException : Thrown if impossible to parse the File at the given path.
     * @throws IOException : Look at Javadoc.
     */
    private void createChannelsFromConnexionFile (String path) throws InterruptedException, FailedRetrievingContainerIPException, ParseException, IOException {
        // We wait until the file has been created
        File f = new File(path);
        int timeout = 10000;
        int elapse = 0;
        while (!f.exists() || elapse >= timeout) {
            Thread.sleep(100);
            elapse += 100;
        }

        // Parse the file to retrieve the interesting informations
        Object file = parser.parse(new FileReader(path));
        JSONObject connexionInfo = (JSONObject) file;

        // Read and save all the network and messaging information from the file
        this.transport = (String) connexionInfo.get("transport");
        this.ip = retrieveContainerIp();
        this.signature_scheme = (String) connexionInfo.get("signature_scheme");
        this.key = (String) connexionInfo.get("key");

        // Retrieve sockets port from the file
        long shell_port = (Long) connexionInfo.get("shell_port");
        long iopub_port = (Long) connexionInfo.get("iopub_port");
        long stdin_port = (Long) connexionInfo.get("stdin_port");
        long hb_port = (Long) connexionInfo.get("hb_port");
        long control_port = (Long) connexionInfo.get("control_port");

        // Create the channels
        this.shell = new ShellChannel("shell", transport, ip, shell_port, containerId, this);
        this.iopub = new IOPubChannel("iopub", transport, ip, iopub_port, containerId, this);
        this.stdin = new StdinChannel("stdin", transport, ip, stdin_port, containerId, this);
        this.hb = new HeartbeatChannel("hb", transport, ip, hb_port, containerId, this);
        this.control = new ShellChannel("control", transport, ip, control_port, containerId, this);
    }

    /* =================================================================================================================
                                                    SETTERS AND GETTERS FUNCTIONS
       ===============================================================================================================*/

    /**
     * The container id is an ID associated to a running container by the docker daemon
     * @return : the id of the running container associated to the instance of kernel.
     */
    public String getContainerId () {
        return this.containerId;
    }

    /**
     * The session is a String only used to communicate on the Jupyter messaging protocol
     * @return : the String session
     */
    public String getSession () { return this.session != null ? this.session : ""; }

    /**
     * The session is a String only used to communicate on the Jupyter messaging protocol
     *
     * @param session : the new session string to use
     */
    public void setSession(String session) { this.session = session; }

    /**
     * The identity is a String only used to communicate on the Jupyter messaging protocol
     * @return : String containing the identity
     */
    public String getIdentity () { return this.identity != null ? this.identity : ""; }

    /**
     * The identity is a String only used to communicate on the Jupyter messaging protocol
     * @param identity (String) the new identity to use
     */
    public void setIdentity (String identity) {
        this.identity = identity;
        shell.setIdentity(identity);
        iopub.setIdentity(identity);
        hb.setIdentity(identity);
        control.setIdentity(identity);
        stdin.setIdentity(identity);
    }

    /**
     * The signature scheme is a parameter to build the HMAC used to communicate on the Jupyter message protocol.
     * Google it for more information, it is a very common concept.
     * @return (String) containing the signature scheme
     */
    public String getSignatureScheme () { return signature_scheme != null ? signature_scheme : ""; }

    /**
     * The key to give to the algorithm creating the HMAC
     * @return (String) the key to use
     */
    public String getKey () { return this.key != null ? this.key : ""; }

    /**
     * nbExecutions is the number of executions the kernel did.
     * Called only by the JupyterMessaging.manager to update the nb of execution when receiving an execute_reply msg.
     * @param nbExecutions : the new number of execution. Usually nbExecution + 1
     */
    public void setNbExecutions (Long nbExecutions) { this.nbExecutions = nbExecutions; }

    /**
     * nbExecutions is the number of executions the kernel did.
     * @return (Long) the nb of executions
     */
    public Long getNbExecutions () { return nbExecutions; }

    /**
     * Tells whether the Kernel is idle or running.
     * Idle means the shell is not doing calculations.
     * @return : true if idle
     */
    public boolean isIdle () { return idle; }

    /**
     * Tells whether the Kernel is idle or running.
     * Busy means the shell is doing calculations.
     * @return true is running (= busy)
     */
    public boolean isBusy () { return !idle; }

    /**
     * Set the idle state of the Kernel.
     * Must only be called by the JupyterMessaging.manager when receiving a message on IOPub.
     * @param value
     */
    public void setIdleState (boolean value) {
        idle = value ;
    }

    /**
     * Get the message manager. It is used to communicate with the Jupyter kernel.
     * @return {Manager} the manager used by the kernel.
     */
    public Manager getMessagesManager () { return messagesManager != null ? messagesManager : new Manager(this); }

    /* =================================================================================================================
                                                    UTILITY FUNCTIONS
       ===============================================================================================================*/

    /**
     * Regex to know if the given text is an IP Address or not.
     * @param text (String) the text that you want to determine if it is an IP Address
     * @return True if the given text is an IP Address
     */
    private boolean isIpAddress(String text) {
        // Define the REGEX for an ip address
        String IPADDRESS_PATTERN =
                "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
                        "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
                        "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
                        "([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";

        // Create a pattern object used to run the regex
        Pattern pattern = Pattern.compile(IPADDRESS_PATTERN);

        // Create an object that verify whether the given "text" match the regex previously given
        Matcher matcher = pattern.matcher(text);

        // Run and return the test
        return matcher.matches();
    }

    /**
     * Retrieve the IP of the Docker container linked to this Kernel.
     * Use bash commands to do that.
     * @return (String) the IP address of the container
     * @throws FailedRetrievingContainerIPException : exception telling that it was unable to retrieve the IP.
     */
    private String retrieveContainerIp() throws FailedRetrievingContainerIPException {
        // Path to the script used to get a running container's ip address
        String pathToScript = Root.PATH_TO_PROJECT_STORAGE + "/retrieve-container-ip.sh";

        try {
            ProcessBuilder pb = new ProcessBuilder( pathToScript, containerId);

            // Runs the command to get its result (the ip address)
            Process proc = pb.start();

            // Retrieve the inputstreams to read the result
            BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            String line;

            // Go through the input stream (that correspond to the standard output from the executed script)
            while((line = in.readLine()) != null) {
                if (isIpAddress(line)) return line;
            }

        } catch (Exception e) {
            throw new FailedRetrievingContainerIPException(this.containerId);
        }

        // If not found we return the localhost
        return "127.0.0.1";
    }

    /**
     * Delete the connexion file.
     * This method is used when stopping the kernel, to make sur that, in case the container did not, we delete
     * the connexion file.
     */
    private void deleteConnexionFile () {
        String absolutePathToConnexionInfoFile = pathToConnexionFilesFolder + "/" + containerId + ".json";

        // We wait until the file has been created
        File f = new File(absolutePathToConnexionInfoFile);
        if (f.exists()) f.delete();
    }

    /* =================================================================================================================
                                                    EXCEPTION CLASSES
       ===============================================================================================================*/

    public class FailedKernelStartException extends Exception {
        public FailedKernelStartException() {
            super("[ERROR] Failed trying to start the docker container. Please verify that your docker daemon is running");
        }
    }

    public class FailedRetrievingContainerIPException extends Exception {
        public FailedRetrievingContainerIPException(String containerId) {
            super("Unable to retrieve ip of container with ID : "
                    + containerId + ". Verify that this container is running");
        }
    }
}
