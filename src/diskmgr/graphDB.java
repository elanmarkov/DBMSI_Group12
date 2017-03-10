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

public class graphDB extends DB {
	static int numGraphDB = 0;
	NodeHeapFile nodes;
	EdgeHeapFile edges;
	BTreeFile nodeLabels;
	ZCurve nodeDesc;
	BTreeFile edgeLabels;
	BTreeFile edgeWeights;
	int type; // Clustering/indexing strategy descriptor
	final int KEY_SIZE = 80;
	
	/**
	Default constructor. Type specifies parameters for keytype and delete_fashion
	0 for integer key, naive delete
	1 for integer key, full delete
	2 for string key, naive delete
	3 for string key, full delete
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
		String filename = "GraphDB" + numGraphDB;
		numGraphDB++;
		nodes = new NodeHeapFile(filename + "NODES");
		edges = new EdgeHeapFile(filename + "EDGES");
		nodeDesc = new ZCurve(filename + "NODEDESC");
		switch(type) {
		case 0:	
		// integer key, naive delete
			nodeLabels = new BTreeFile(filename + "NODELABEL", 0, KEY_SIZE, 0);
			edgeLabels = new BTreeFile(filename + "EDGELABEL", 0, KEY_SIZE, 0);
			edgeWeights = new BTreeFile(filename + "EDGEWEIGHT", 0, KEY_SIZE, 0);
			break;
		case 1:
		// integer key, full delete
			nodeLabels = new BTreeFile(filename + "NODELABEL", 0, KEY_SIZE, 1);
			edgeLabels = new BTreeFile(filename + "EDGELABEL", 0, KEY_SIZE, 1);
			edgeWeights = new BTreeFile(filename + "EDGEWEIGHT", 0, KEY_SIZE, 1);
			break;
		case 2:
		// string key, naive delete
			nodeLabels = new BTreeFile(filename + "NODELABEL", 1, KEY_SIZE, 0);
			edgeLabels = new BTreeFile(filename + "EDGELABEL", 1, KEY_SIZE, 0);
			edgeWeights = new BTreeFile(filename + "EDGEWEIGHT", 1, KEY_SIZE, 0);
			break;
		case 3:
		// string key, full delete
			nodeLabels = new BTreeFile(filename + "NODELABEL", 1, KEY_SIZE, 1);
			edgeLabels = new BTreeFile(filename + "EDGELABEL", 1, KEY_SIZE, 1);
			edgeWeights = new BTreeFile(filename + "EDGEWEIGHT", 1, KEY_SIZE, 1);
			break;
		default:
			throw new ClassCastException("No such type for keytype/delete_fashion (in GraphDB.java)");
		}
		// initialize each method; to be implemented when combined
	}

	public int getNodeCnt() throws HFBufMgrException, InvalidSlotNumberException, InvalidTupleSizeException, IOException {
		return nodes.getNodeCnt();
	}
	
	public int getEdgeCnt() throws HFBufMgrException, InvalidSlotNumberException, IOException, InvalidTupleSizeException {
		return edges.getEdgeCnt();
	}
	
	public int getSourceCnt() {
		return 0;
		//return ???
	}

	public int getDestinationCnt() {
		return 0;
		//return ???
	}

	public int getLabelCnt() {
		return 0;
		//return ???
	}
}
