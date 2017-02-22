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

public class graphDB extends DB {
	//NodeHeapFile nodes;
	//EdgeHeapFile edges;
	BT nodeLabels;
	//ZT nodeDesc;
	BT edgeLabels;
	BT edgeWeights;
	
	public graphDB() {
		super();
		// initialize each method; to be implemented when combined
	}

	public int getNodeCnt() {
		return 0;
		//return nodes.getNodeCnt();
	}
	
	public int getEdgeCnt() {
		return 0;
		//return edges.getEdgeCnt();
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
