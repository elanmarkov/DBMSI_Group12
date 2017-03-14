/*
GraphDB class by Elan Markov

This class maintains all files relevant to the implementation
of a graph database.
*/
package diskmgr;

import java.io.*;
import bufmgr.*;
import global.*;
import btree.*;
import zindex.*;
import heap.*;
import java.util.ArrayList;

/** Node Reference class.
A token that tracks the number of occurrences of a specific NID. Used for bookkeeping. */
class nodeRef{
	public NID node;
	int ref;
	/** Constructor for nodeRef class. */
	public nodeRef(NID node) {
		this.node = node;
		ref = 1;
	}
}

/** Label Reference class.
A token that tracks the number of occurrences of a specific label. Used for bookkeeping. */
class labelRef{
	public String label;
	int ref;
	/** Constructor for labelRef class. */
	public labelRef(String label) {
		this.label = label;
		ref = 1;
	}
}

/** Graph Database class.
Extends the original relational database into a graph database. 
Contains nodes and edges that create the graph, along with labels (for both nodes and edges), edge weights, and node descriptors.
Performs bookkeeping of all source and destination nodes in order to return getSourceCnt and getDestinationCnt queries more effectively.
Contains query handlers in order to control access to internal data structures - tests will call query handlers to perform necessary operations.
*/
public class graphDB extends DB {
	static int numGraphDB = 0;//Global tracker of DB identification number, for file names.

	// Core containers
	NodeHeapFile nodes;
	EdgeHeapFile edges;
	BTreeFile nodeLabels;
	ZCurve nodeDesc;
	BTreeFile edgeLabels;
	BTreeFile edgeWeights;

	// Unique format of filename to be randomly generated
	String filename;

	// Query handlers
	NodeQueryHandler nodeQuery;
	EdgeQueryHandler edgeQuery;
	BatchNodeDeleteHandler batchNodeDelete;
	BatchEdgeDeleteHandler batchEdgeDelete;
	BatchNodeInsertHandler batchNodeInsert;
	BatchEdgeInsertHandler batchEdgeInsert;

	// Track unique source/destination/label values
	ArrayList<nodeRef> sourceNodes;
	ArrayList<nodeRef> destNodes;
	ArrayList<labelRef> labelNames;
	int type; // Clustering/indexing strategy descriptor
	int graphNum;//Local graph ID number
	final int KEY_SIZE = 80;
	
	/**
	Constructor for graphDB. Type specifies parameter for delete_fashion
	1 for full delete
	0 or any other value for naive delete (default)

	Initializes the DB itself and all basic structures.
	Note that the NodeHeapFile and other data structures cannot be initialized
	until the database exists, so they are not initialized in the constructor.
	After the constructor is invoked, must invoke init() to initialize the remaining
	graphDB components (will otherwise receive NullPointerException).
	*/
	public graphDB(int type) 
		throws InvalidSlotNumberException, 
	   	InvalidTupleSizeException, 
	   	HFException,
	   	HFBufMgrException,
	   	HFDiskMgrException,
	   	Exception {
		super();
		this.type = type; 
		graphNum = numGraphDB;
		filename = "GraphDB" + graphNum;
		numGraphDB++;
		sourceNodes = new ArrayList<nodeRef>();
		destNodes = new ArrayList<nodeRef>();
		labelNames = new ArrayList<labelRef>();
	}
	/** 
	Initializes all remaining methods not initialized in the constructor.
	Constructs the BTrees according to the specified delete fashion. 
	Initializes the handler functions.
	*/
	public void init() throws InvalidSlotNumberException, 
	   	InvalidTupleSizeException, 
	   	HFException,
	   	HFBufMgrException,
	   	HFDiskMgrException,
	   	Exception {
		nodes = new NodeHeapFile(null);
		edges = new EdgeHeapFile(null);
		nodeDesc = new ZCurve(filename + "NODEDESC");
		if(type == 1) {
		// full delete
			nodeLabels = new BTreeFile(filename + "NODELABEL", AttrType.attrString, KEY_SIZE, 1);
			edgeLabels = new BTreeFile(filename + "EDGELABEL", AttrType.attrString, KEY_SIZE, 1);
			edgeWeights = new BTreeFile(filename + "EDGEWEIGHT", AttrType.attrInteger, KEY_SIZE, 1);
		}
		else {
		// Otherwise, naive delete
			nodeLabels = new BTreeFile(filename + "NODELABEL", AttrType.attrString, KEY_SIZE, 0);
			edgeLabels = new BTreeFile(filename + "EDGELABEL", AttrType.attrString, KEY_SIZE, 0);
			edgeWeights = new BTreeFile(filename + "EDGEWEIGHT", AttrType.attrInteger, KEY_SIZE, 0);
		}
		nodeQuery = new NodeQueryHandler(nodes, edges, nodeLabels, nodeDesc, edgeLabels, edgeWeights, this);
		edgeQuery = new EdgeQueryHandler(nodes, edges, nodeLabels, nodeDesc, edgeLabels, edgeWeights, this);
		batchNodeDelete = new BatchNodeDeleteHandler(nodes, edges, nodeLabels, nodeDesc, edgeLabels, edgeWeights, this);
		batchEdgeDelete = new BatchEdgeDeleteHandler(nodes, edges, nodeLabels, nodeDesc, edgeLabels, edgeWeights, this);
		batchNodeInsert = new BatchNodeInsertHandler(nodes, edges, nodeLabels, nodeDesc, edgeLabels, edgeWeights, this);
		batchEdgeInsert = new BatchEdgeInsertHandler(nodes, edges, nodeLabels, nodeDesc, edgeLabels, edgeWeights, this);
	}
	/** Getter function that gives current node count. */
	public int getNodeCnt() throws HFBufMgrException, InvalidSlotNumberException, InvalidTupleSizeException, IOException, HFDiskMgrException {
		return nodes.getNodeCnt();
	}
	/** Getter function that gives current edge count. */
	public int getEdgeCnt() throws HFBufMgrException, InvalidSlotNumberException, IOException, InvalidTupleSizeException, HFDiskMgrException {
		return edges.getEdgeCnt();
	}
	/** Getter function that gives current unique source node count. */
	public int getSourceCnt() {
		return sourceNodes.size(); 
	}
	/** Getter function that gives current unique destination node count. */
	public int getDestinationCnt() {
		return destNodes.size();
	}
	/** Getter function that gives current unique label count. */
	public int getLabelCnt() {
		return labelNames.size();
	}
	/** Inserts a node into the graphDB and performs all relevant bookkeeping. */
	public void insertNode(Node node) throws IOException, Exception {
		NID id = nodes.insertNode(node.getNodeByteArray());
		nodeLabels.insert(new StringKey(node.getLabel()), id);
		nodeDesc.insert(new DescriptorKey(node.getDesc()), id);
		addLabelNoDuplicate(labelNames, node.getLabel());
		return;
	}
	/** Inserts an edge into the graphDB and performs all relevant bookkeeping. */
	public void insertEdge(Edge edge) throws IOException, Exception {
		EID id = edges.insertEdge(edge.getEdgeByteArray());
		edgeLabels.insert(new StringKey(edge.getLabel()), id);
		edgeWeights.insert(new IntegerKey(edge.getWeight()), id);
		addNodeNoDuplicate(sourceNodes, edge.getSource());
		addNodeNoDuplicate(destNodes, edge.getDestination());
		addLabelNoDuplicate(labelNames, edge.getLabel());
		return;
	}
	/** Deletes a node from the graphDB and performs all relevant bookkeeping. */
	public void deleteNode(NID id) throws IOException, Exception {
		Node node = nodes.getNode(id);
		nodeLabels.Delete(new StringKey(node.getLabel()), id);
		nodeDesc.Delete(new DescriptorKey(node.getDesc()), id);
		removeLabel(labelNames, node.getLabel());
		nodes.deleteNode(id);
		return;
	}
	/** Deletes an edge from the graphDB and performs all relevant bookkeeping. */
	public void deleteEdge(EID id) throws IOException, Exception {
		Edge edge = edges.getEdge(id);
		edgeLabels.Delete(new StringKey(edge.getLabel()), id);
		edgeWeights.Delete(new IntegerKey(edge.getWeight()), id);
		removeNode(sourceNodes, edge.getSource());
		removeNode(destNodes, edge.getDestination());
		removeLabel(labelNames, edge.getLabel());
		edges.deleteEdge(id);
		return;
	}
	/** Getter function that returns the handler for the given query. */	
	public NodeQueryHandler getNodeQueryHandler() {
		return nodeQuery;
	}
	/** Getter function that returns the handler for the given query. */	
	public EdgeQueryHandler getEdgeQueryHandler() {
		return edgeQuery;
	}
	/** Getter function that returns the handler for the given query. */	
	public BatchNodeDeleteHandler getBatchNodeDeleteHandler() {
		return batchNodeDelete;
	}
	/** Getter function that returns the handler for the given query. */	
	public BatchEdgeDeleteHandler getBatchEdgeDeleteHandler() {
		return batchEdgeDelete;
	}
	/** Getter function that returns the handler for the given query. */	
	public BatchNodeInsertHandler getBatchNodeInsertHandler() {
		return batchNodeInsert;
	}
	/** Getter function that returns the handler for the given query. */	
	public BatchEdgeInsertHandler getBatchEdgeInsertHandler() {
		return batchEdgeInsert;
	}
	/** Adds a nodeRef to the given ArrayList. If it is a duplicate, it will simply increment the number of references. */
	private void addNodeNoDuplicate(ArrayList<nodeRef> list, NID newNode) {
		boolean duplicate = false;
		nodeRef item;
		int iter = 0;		
		while(!duplicate && iter < list.size()){
			item = list.get(iter);
			if(item.node.equals(newNode)) {
				duplicate = true;
				item.ref++;
			}
			else {
				iter++;
			}
		}
		if(!duplicate) {
			list.add(new nodeRef(newNode));
		}
	}
	/** Adds a labelRef to the given ArrayList. If it is a duplicate, it will simply increment the number of references. */
	private void addLabelNoDuplicate(ArrayList<labelRef> list, String newLabel) {
		boolean duplicate = false;
		labelRef item;
		int iter = 0;		
		while(!duplicate && iter < list.size()){
			item = list.get(iter);
			if(item.label.equals(newLabel)) {
				duplicate = true;
				item.ref++;
			}
			iter++;
		}
		if(!duplicate) {
			list.add(new labelRef(newLabel));
		}
	}
	/** Removes one instance of the given node from the given ArrayList, and deletes the entry if the number
	of references becomes zero. 
	Returns true if a deletion occurred; false if item not found. */
	private boolean removeNode(ArrayList<nodeRef> list, NID rmNode) {
		boolean found = false;
		int iter = 0;
		nodeRef item;		
		while(!found && iter < list.size()){
			item = list.get(iter);
			if(item.node.equals(rmNode)) {
				found = true;
				item.ref--;
				if(item.ref==0) {
					list.remove(iter); // Delete unreferenced object
				}
			}
			iter++;
		}
		return found;
	}
	/** Removes one instance of the given label from the given ArrayList, and deletes the entry if the number
	of references becomes zero. 
	Returns true if a deletion occurred; false if item not found. */
	private boolean removeLabel(ArrayList<labelRef> list, String rmLabel) {
		boolean found = false;
		int iter = 0;
		labelRef item;		
		while(!found && iter < list.size()){
			item = list.get(iter);
			if(item.label.equals(rmLabel)) {
				found = true;
				item.ref--;
				if(item.ref==0) {
					list.remove(iter); // Delete unreferenced object
				}
			}
			iter++;
		}
		return found;
	}
}
