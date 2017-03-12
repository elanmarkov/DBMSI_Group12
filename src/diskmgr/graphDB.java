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

class nodeRef{
// Tracks number of references to a given node.
	public NID node;
	int ref;
	public nodeRef(NID node) {
		this.node = node;
		ref = 1;
	}
}
class labelRef{
	public String label;
	int ref;
	public labelRef(String label) {
		this.label = label;
		ref = 1;
	}
}

public class graphDB extends DB {
	static int numGraphDB = 0;//Global tracker of DB identification number, for file names.
	NodeHeapFile nodes;
	EdgeHeapFile edges;
	BTreeFile nodeLabels;
	ZCurve nodeDesc;
	BTreeFile edgeLabels;
	BTreeFile edgeWeights;
	String filename;
	NodeQueryHandler nodeQuery;

	// Track unique source/destination/label values
	ArrayList<nodeRef> sourceNodes;
	ArrayList<nodeRef> destNodes;
	ArrayList<labelRef> labelNames;
	int type; // Clustering/indexing strategy descriptor
	int graphNum;//Local graph ID number
	final int KEY_SIZE = 80;
	
	/**
	Default constructor. Type specifies parameters for keytype and delete_fashion
	0 for naive delete
	1 for full delete
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
	public void init() {
		nodes = new NodeHeapFile(null);
		edges = new EdgeHeapFile(null);
		nodeDesc = new ZCurve(filename + "NODEDESC");
		if(type == 1) {
		// full delete
			nodeLabels = new BTreeFile(filename + "NODELABEL", 1, KEY_SIZE, 1);
			edgeLabels = new BTreeFile(filename + "EDGELABEL", 1, KEY_SIZE, 1);
			edgeWeights = new BTreeFile(filename + "EDGEWEIGHT", 0, KEY_SIZE, 1);
		}
		else {
		// Otherwise, naive delete
			nodeLabels = new BTreeFile(filename + "NODELABEL", 1, KEY_SIZE, 0);
			edgeLabels = new BTreeFile(filename + "EDGELABEL", 1, KEY_SIZE, 0);
			edgeWeights = new BTreeFile(filename + "EDGEWEIGHT", 0, KEY_SIZE, 0);
		}
		nodeQuery = new NodeQueryHandler(nodes, edges, NodeLabels, NodeDesc, edgeLabels, edgeWeights);
	}
	public int getNodeCnt() throws HFBufMgrException, InvalidSlotNumberException, InvalidTupleSizeException, IOException, HFDiskMgrException {
		return nodes.getNodeCnt();
	}
	
	public int getEdgeCnt() throws HFBufMgrException, InvalidSlotNumberException, IOException, InvalidTupleSizeException, HFDiskMgrException {
		return edges.getEdgeCnt();
	}
	
	public int getSourceCnt() {
		return sourceNodes.size(); 
	}

	public int getDestinationCnt() {
		return destNodes.size();
	}

	public int getLabelCnt() {
		return labelNames.size();
	}
	public void insertNode(Node node) throws IOException, Exception {
		NID id = nodes.insertNode(node.getNodeByteArray());
		nodeLabels.insert(new StringKey(node.getLabel()), id);
		nodeDesc.insert(new StringKey(node.getDesc().toString()), id);
		addLabelNoDuplicate(labelNames, node.getLabel());
		return;
	}
	public void insertEdge(Edge edge) throws IOException, Exception {
		EID id = edges.insertEdge(edge.getEdgeByteArray());
		edgeLabels.insert(new StringKey(edge.getLabel()), id);
		edgeWeights.insert(new IntegerKey(edge.getWeight()), id);
		addNodeNoDuplicate(sourceNodes, edge.getSource());
		addNodeNoDuplicate(destNodes, edge.getDestination());
		addLabelNoDuplicate(labelNames, edge.getLabel());
		return;
	}
	public void deleteNode(NID id) throws IOException, Exception {
		Node node = nodes.getNode(id);
		nodeLabels.Delete(new StringKey(node.getLabel()), id);
		nodeDesc.Delete(new StringKey(node.getDesc().toString()), id);
		removeLabel(labelNames, node.getLabel());
		nodes.deleteNode(id);
		return;
	}
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
	public NodeQueryHandler getNodeQueryHandler() {
		return nodeQuery;
	}
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
