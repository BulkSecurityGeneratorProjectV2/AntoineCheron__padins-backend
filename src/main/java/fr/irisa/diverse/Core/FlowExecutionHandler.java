package fr.irisa.diverse.Core;

import fr.irisa.diverse.FBPNetworkProtocolUtils.Status;
import fr.irisa.diverse.Flow.Flow;
import fr.irisa.diverse.Flow.Group;
import fr.irisa.diverse.Flow.Node;
import fr.irisa.diverse.Utils.Utils;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Handle the execution of one group or flow.
 *
 * You have to create one instance of this class per flow you want to run.
 * Then, when you want to run a flow, it works has following :
 * 1 - Search for the first Nodes to execute (the ones with no inputs connected)
 * 2 - Run these first nodes.
 *      To do that, we put all the nodes to execute in a Set.
 *      Beside that, we have a master that look at each node in the Set and run it if possible.
 *      The execution of a node is done in a new thread.
 *      After starting the execution of a node, it put the thread into a Running Set.
 *      When the execution of a Node in a thread finish, it adds the nodes following the one that just runned into the
 *      toLaunch Set.
 *      In order to run a node, the master verify that every dependency have finished their execution. If not, it continues
 *      going through the Set, looking for nodes that can run.
 *      When both Set (toLaunch and running) are empty, we stop the master and the execution of the flow is finished.
 *
 * Created by antoine on 02/06/17.
 */
public class FlowExecutionHandler {

    // Attributes
    private ArrayList<Node> nodes;
    private Workspace owningWorkspace;
    private Flow flow;
    private Status status;

    private Set<Node> toLaunch;
    private Set<NodeExecutionThread> running;
    private boolean stop;

    /*==================================================================================================================
                                                    CONSTRUCTOR
     =================================================================================================================*/
    public FlowExecutionHandler (String graph, Workspace owningWorkspace, Flow flow) {
        this.owningWorkspace = owningWorkspace;
        this.flow = flow;
        this.toLaunch = new ConcurrentSkipListSet<>();
        this.running = new ConcurrentSkipListSet<>();
        this.stop = false;

        Object o = flow.getGraph(graph);

        if (o instanceof Flow) {
            nodes = ((Flow) o).getNodes();
            status = ((Flow) o).getStatus();
        } else if (o instanceof  Group){
            nodes = flow.getNodes((Group) o);
            status = ((Group) o).getStatus();
        }
    }

    /*==================================================================================================================
                                                    PUBLIC CLASS METHODS
     =================================================================================================================*/

    /**
     * Start the execution of the flow given to the constructor
     */
    public void run () {
        prepareNodesForExecution();

        runNodes();
    }

    /**
     * Stop the flow's execution
     */
    public void stop () {
        stopNodes(nodes);
    }

    /**
     * Add a node to the list of nodes that will be started as soon as possible
     *
     * @param n : the Node to add
     */
    synchronized public void addToLaunch (Node n) {
        this.toLaunch.add(n);
    }

    /**
     * Method for the Thread to prevent that it finished.
     * It will remove it from the list of running nodes (1 node <-> 1 thread).
     *
     * @param t : the Thread that finished.
     */
    public void runningThreadFinished (Thread t) {
        running.remove(t);
    }

    /*==================================================================================================================
                                                GETTERS AND SETTERS
     =================================================================================================================*/

    /**
     * @return a boolean telling whether the Execution of the Flow is running or not
     */
    public boolean isRunning () {
        return status.isRunning();
    }

    /*==================================================================================================================
                                              PRIVATE CLASS METHODS
     =================================================================================================================*/

    /**
     * Actually starts the flow to execute.
     *
     * It starts with retrieve the first nodes to execute.
     * Then put them into the toLaunch set and start doing the master job, as described above.
     */
    private void runNodes () {
        // Retrieve the first nodes to execute
        ArrayList<Node> firstNodes = flow.findFirstNodesOfFlow(nodes);

        // Add each first node to the toLaunch list
        for( Node n : firstNodes) {
            toLaunch.add(n);
        }

        // Tell the status that we started
        status.start();

        // Start a while that look at the toLaunch list and start running a Node as soon as possible.
        while ((!toLaunch.isEmpty() || !running.isEmpty()) && !stop) {
            for (Node n : toLaunch) {
                // Verify that all the previous nodes in the flow have finished their execution
                if (havePreviousNodesFinish(n)) {
                    // If so, start running it
                    runNode(n);
                    toLaunch.remove(n);
                }
            }
        }

        // Here it is finished, we change the status
        status.stop();
    }

    /**
     * Stop the execution of the given nodes
     *
     * @param nodes : The List of nodes to stop
     */
    private void stopNodes (ArrayList<Node> nodes) {
        // First : set stop to true to stop the while in runNodes
        this.stop = true;
        // Second : interrupt the Thread and remove them from the set.
        for (Thread t : running) {
            t.interrupt();
            running.remove(t);
        }

        // Third : make sure the nodes have been stopped
        for (Node n : nodes) {
            owningWorkspace.stopNode(n);
        }

        // Finally empty the toLaunch set
        toLaunch = new ConcurrentSkipListSet<>();
        running = new ConcurrentSkipListSet<>();
    }

    private void prepareNodesForExecution () {
        for (Node n : nodes) {
            n.prepareForExecution();
        }
    }

    /**
     * Run a unique node.
     * It starts a new Thread for the node and add it to the Running set.
     * @param node : the Node to execute
     */
    private void runNode (Node node) {

        // If the node is running, we kill it
        if (node.isRunning()){
            owningWorkspace.stopNode(node);
        } else {
            // Create and run the thread
            NodeExecutionThread t = new NodeExecutionThread(node, this, owningWorkspace);
            running.add(t);
            t.start();
        }

    }

    /**
     * Tells whether the dependency of a node finished running.
     * This method is usually called in order to know if it is possible de run a node.
     *
     * @param n : the Node for which you want to know if dependencies finished running.
     * @return True if all the dependencies finished
     */
    private boolean havePreviousNodesFinish(Node n) {
        // Retrieve the previousNodes of the given node n.
        ArrayList<Node> previousNodes = n.previousInFlow();
        boolean res = true;

        // Single case : if there is no previous node, we consider that previous ones have finished,
        // because we can start the execution of this node.
        if (previousNodes == null) return true;
        else {
            // Most common case
            for (Node previous : previousNodes) {
                // For each previous node, we look if the node has finished. To determine that, this previous node
                // also take a look at its dependencies, that look at theirs and so one.
                // Thanks to that we make sure that we return true only if all previous node finished their execution,
                // not just the previous one. It reduces the probability of error.
                // Then we do an arithmetic operation to store the result.
                res = res && previous.hasFinished();
            }

            // End
            return res;
        }
    }
} // End class