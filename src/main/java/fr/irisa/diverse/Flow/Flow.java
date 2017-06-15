package fr.irisa.diverse.Flow;

import fr.irisa.diverse.Core.Workspace;
import fr.irisa.diverse.FBPNetworkProtocolUtils.Status;
import fr.irisa.diverse.Utils.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;

/** A flow is the JSON file containing all the data structure of a workspace.
 * The web interface uses it, and only it, to create the view.
 *
 * Created by antoine on 26/05/2017.
 */
public class Flow implements FlowInterface {

    // Attributes
    private JSONObject flow = null;
    public Workspace owningWorkspace = null;
    // The below attributes have to be contained into the flow object.
    private String id = "";
    private String description = "";
    private String componentsLibrary = "";
    private ArrayList<Edge> edges = null;
    private ArrayList<Node> nodes = null;
    private ArrayList<Group> groups = null;
    private Status status = null;

    // Constructor
    public Flow (Workspace workspace) {
        this.id = workspace.getUuid();
        this.owningWorkspace = workspace;
        this.flow = new JSONObject();

        this.edges = new ArrayList<>();
        this.nodes = new ArrayList<>();
        this.groups = new ArrayList<>();

        componentsLibrary = owningWorkspace.getLibrary();

        this.status = new Status();
    }

    /** Constructor that creates a fr.irisa.diverse.Flow from a JSONObject.
     * Use case : after server restart, re-create workspaces from the saved JSON files.
     *
     * @param source : the parsed file
     */
    public Flow (JSONObject source, Workspace workspace) {
        this.flow = source;
        this.status = new Status();
        this.owningWorkspace = workspace;

        this.id = source.get("id") != null ? (String) source.get("id") : "";
        this.componentsLibrary = source.get("library") != null ? (String) source.get("library") : "";
        this.description = source.get("description") != null ? (String) source.get("description") : "";
        this.edges = source.get("edges") != null ? (ArrayList) source.get("edges") : new ArrayList<>();
        this.nodes = source.get("nodes") != null ? (ArrayList) source.get("nodes") : new ArrayList<>();
        this.groups = source.get("groups") != null ? (ArrayList) source.get("groups") : new ArrayList<>();
    }

    /* =================================================================================================================
                                                    PUBLIC FUNCTIONS
       ===============================================================================================================*/

    /**
     * Serialize the Flow as a JSON and return it
     *
     * @return a JSON representation of the flow
     */
    public String serialize () {
        // Preliminary step : build JSONArray for edges, nodes and groups
        JSONArray edges = JSON.jsonArrayFromArrayList(this.edges);
        JSONArray nodes = JSON.jsonArrayFromArrayList(this.nodes);
        JSONArray groups = JSON.jsonArrayFromArrayList(this.groups);
        // Build the JSON file of the flow
        flow.put("id", id);
        flow.put("name", owningWorkspace.getName());
        flow.put("library", componentsLibrary);
        flow.put("description", description);
        flow.put("edges", edges);
        flow.put("nodes", nodes);
        flow.put("groups", groups);

        // Return it as a JSON String to send it to frontend
        return flow.toJSONString();
    }

    public boolean addNode(String id, String component, JSONObject metadata, String graph, boolean executable) {
        if (graphExist(graph) && !nodeExist(id)){
            Node n = new Node(id, component, metadata, graph, executable, this);

            return nodes.add(n);
        }

        return false;
    }

    public boolean removeNode(String id, String graph) {
        // Verify that the requested graph is the workspace
        if(graphExist(graph) && nodeExist(id)) {
            // If so, retrieve the index of the node and remove it
            nodes.remove(indexOfNode(id));
            return true;
        } else {
             return false;
        }
    }

    public boolean renameNode(String from, String to, String graph) {
        // Verify that the requested graph is the workspace
        if(graphExist(graph) && nodeExist(from)) {
            // If so, retrieve the node and modify its id
            Node n = nodes.get(indexOfNode(from));
            n.setId(to);
            return true;
        } else {
            return false;
        }
    }

    public boolean changeNode(String id, JSONObject metadata, String graph) {
        // Verify that the requested graph is the workspace
        if(graphExist(graph) && nodeExist(id)) {
            // If so, retrieve the node and modify its id
            Node n = nodes.get(indexOfNode(id));
            n.setMetadata(metadata);
            return true;
        } else {
            return false;
        }
    }

    public boolean addEdge (JSONObject src, JSONObject tgt, JSONObject metadata, String graph) {
        String srcNodeId = (String) src.get("node");
        String tgtNodeId = (String) tgt.get("node");

        if(nodeExist(srcNodeId) && nodeExist(tgtNodeId) && graphExist(graph) && !edgeExist(src, tgt)) {
            Edge newEdge = new Edge(src, tgt, metadata, graph, this);
            edges.add(newEdge);

            Node srcNode = nodes.get(indexOfNode(srcNodeId));
            srcNode.assignPortToEdge((String) src.get("port"), newEdge.getId());

            Node tgtNode = nodes.get(indexOfNode(tgtNodeId));
            tgtNode.assignPortToEdge((String) tgt.get("port"), newEdge.getId());

            return true;
        } else {
            return false;
        }
    }

    public boolean removeEdge(String graph, JSONObject src, JSONObject tgt) {
        // Verify that the requested graph is the workspace
        if(graphExist(graph) && edgeExist(src, tgt)) {
            // If so, retrieve the index of the edge and remove it
            edges.remove(indexOfEdge(src, tgt));
            return true;
        } else {
            return false;
        }
    }

    public boolean changeEdge(String graph, JSONObject metadata, JSONObject src, JSONObject tgt) {
        // Verify that the requested graph is the workspace
        if(graphExist(graph) && edgeExist(src, tgt)) {
            // If so, retrieve the edge and modify its metadata
            Edge e = edges.get(indexOfEdge(src, tgt));
            e.setMetadata(metadata);
            return true;
        } else {
            return false;
        }
    }

    public boolean addInitial(String graph, JSONObject metadata, JSONObject src, JSONObject tgt) {
        // Not used for now
        return true;
    }

    public boolean removeInitial(String graph, JSONObject src, JSONObject tgt) {
        // Not used for now
        return true;
    }

    public boolean addInport(String name, String node, String port, JSONObject metadata, String graph) {
        // Not used for now
        return true;
    }

    public boolean removeInport(String name, String graph) {
        // Not used for now
        return true;
    }

    public boolean renameInport(String from, String to, String graph) {
        // Not used for now
        return true;
    }

    public boolean addOutport(String name, String node, String port, JSONObject metadata, String graph) {
        // Not used for now
        return true;
    }

    public boolean removeOutport(String name, String graph) {
        // Not used for now
        return true;
    }

    public boolean renameOutport(String from, String to, String graph) {
        // Not used for now
        return true;
    }

    public boolean addGroup(String name, JSONArray nodes, JSONObject metadata, String graph) {
        if(graphExist(graph) && !groupExist(name)){
            Group g = new Group(name, nodes, metadata, graph, this);

            groups.add(g);
            return true;
        } else {
            return false;
        }
    }

    public boolean removeGroup(String name, String graph) {
        if(graphExist(graph) && groupExist(name)) {
            groups.remove(indexOfGroup(name));
            return true;
        } else {
            return false;
        }
    }

    public boolean renameGroup(String from, String to, String graph) {
        // Verify that the requested graph is the workspace
        if(graphExist(graph) && groupExist(from)) {
            // If so, retrieve the group and modify its name
            Group g = groups.get(indexOfGroup(from));
            g.setName(to);
            return true;
        } else {
            return false;
        }
    }

    public boolean changeGroup(String name, JSONObject metadata, String graph) {
        // Verify that the requested graph is the workspace
        if(graphExist(graph) && groupExist(name)) {
            // If so, retrieve the group and modify its metadata
            Group g = groups.get(indexOfGroup(name));
            g.setMetadata(metadata);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Determine which nodes are the first one on the flow and give the list of them.
     *
     * @param nodes The nodes composing the flow
     * @return The list of first nodes to execute in order to run the flow.
     */
    public ArrayList<Node> findFirstNodesOfFlow (ArrayList<Node> nodes) {
        ArrayList<Node> res = new ArrayList<>();
        // First, in case nodes is composed of only one node, we return the node
        if (nodes.size() == 1 ) return nodes;
        // Elsewhere, we search for the node in the list that doesn't have a previous node and that have a next one.
        for (Node n : nodes) {
            if(n.previousInFlow() == null && n.nextInFlow() != null) res.add(n);
        }

        // Finally return the list
        return res;
    }

    /* =================================================================================================================
                                                    GETTERS AND SETTERS
       ===============================================================================================================*/

    /**
     * The components library is the library that contains all the components the user will be able to use
     * in order to build his flow.
     *
     * @return The name of the library
     */
    public String getComponentsLibrary() {
        return componentsLibrary;
    }

    /**
     * The unique id of the Flow
     * @return The unique id of the Flow as String
     */
    public String getId() {
        return id;
    }


    public Edge getEdge (JSONObject src, JSONObject tgt, String graph) {
        if (graphExist(graph) && edgeExist(src, tgt)) {
            return edges.get(indexOfEdge(src, tgt));
        } else {
            return null;
        }
    }

    public Edge getEdge (String id) {
        // Look at each edge and if its id is the same as the given one, returns it.
        for (int i=0; i<edges.size(); i++) {
            if (edges.get(i).getId().equals(id)) return edges.get(i);
        }

        return null;
    }

    public ArrayList<Node> getNodes() {
        return nodes;
    }

    public ArrayList<Node> getNodes (Group g) {
        JSONArray nodesId = g.getNodes();
        ArrayList<Node> res = new ArrayList<>();

        for (Object o : nodesId) {
            String id = (String) o;
            res.add(getNode(id, this.id));
        }

        return res;
    }

    public Node getNode (String id, String graph) {
        if (graphExist(graph) && nodeExist(id)) {
            return nodes.get(indexOfNode(id));
        } else {
            return null;
        }
    }

    public Group getGroup (String name, String graph) {
        if (graphExist(graph) && groupExist(name)) {
            return groups.get(indexOfGroup(name));
        } else {
            return null;
        }
    }

    public Object getGraph (String graph) {
        if (graph.equals(id)) return this;
        else {
            // It means that graph is a group
            return getGroup(graph, id);
        }
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Status getStatus() {
        return status;
    }

    /* =================================================================================================================
                                                    PRIVATE FUNCTIONS
       ===============================================================================================================*/

    private boolean nodeExist (String id) {
        // Go trough all the nodes and if it finds one with the given id return true, else return false
        for (Node node : nodes) {
            if (id.equals(node.getId())) return true;
        }

        return false;
    }

    private boolean edgeExist (JSONObject src, JSONObject tgt) {
        return indexOfEdge(src, tgt) != -1;
    }

    private boolean graphExist (String id) {
        if (this.id.equals(id)) return true;

        for (Group group : groups) {
            if (id.equals(group.getId())) return true;
        }

        return false;
    }

    private boolean groupExist (String name) {
        if (this.groups == null) return false;

        for (Group group : groups) {
            if (group.getName().equals(name)) return true;
        }

        return false;
    }

    private int indexOfEdge (JSONObject src, JSONObject tgt) {
        for(int i=0; i<edges.size(); i++) {
            if (edges.get(i).getSrc().equals(src) && edges.get(i).getTgt().equals(tgt)) return i;
        }

        return -1;
    }

    private int indexOfNode (String id) {
        for(int i=0; i<nodes.size(); i++) {
            if (nodes.get(i).getId().equals(id)) return i;
        }

        return -1;
    }

    private int indexOfGroup (String name) {
        for(int i=0; i<groups.size(); i++) {
            if (groups.get(i).getName().equals(name)) return i;
        }

        return -1;
    }

}
