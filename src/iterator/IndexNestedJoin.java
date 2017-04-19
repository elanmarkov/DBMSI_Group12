/** Index Nested Loop Join by Elan Markov
CSE 510 Phase 3 Group 12
Task 1 */
package iterator;

import global.*;
import heap.*;
import diskmgr.*;
import bufmgr.*;
import index.*;
import btree.*;
import java.io.*;
import java.util.ArrayList;

public class IndexNestedJoin extends Iterator {
	private static boolean OK = true;
	private static boolean FAIL = false;
	private Heapfile joinHeap;
	private Scan EdgeScan;
	private Scan JoinScan;
	private  BTFileScan indexSearch;
	private boolean status;
	private AttrType      _in1[],  _in2[];
        private   int        in1_len, in2_len;
  	private   Iterator  outer;
  	private   short t2_str_sizescopy[];
  	private   CondExpr OutputFilter[];
  	private   CondExpr RightFilter[];
  	private   int        n_buf_pgs;        // # of buffer pages available.
  	private   Tuple     outer_tuple, inner_tuple;
  	private   Tuple     Jtuple;           // Joined tuple
  	private   FldSpec   perm_mat[];
  	private   int        nOutFlds;
	private IndexNestedJoin leftArg;
/** Constructor for join of a Tuple and edge or node.
Parameters as in NestedLoopsJoins class
IndexNestedJoin as an argument for previous tuple set.
CondExpr for condition on edge/node
typejoin as per switch statement below. */
	public IndexNestedJoin(AttrType    in1[],    
			   int     len_in1,           
			   short   t1_str_sizes[],
			   AttrType    in2[],         
			   int     len_in2,           
			   short   t2_str_sizes[],   
			   int     amt_of_mem,        
			   Iterator     am1,          
			   String relationName,      
			   CondExpr outFilter[],      
			   CondExpr rightFilter[],    
			   FldSpec   proj_list[],
			   int        n_out_flds,
			   IndexNestedJoin arg, CondExpr condition, int typejoin) 
		throws Exception {
		switch(typejoin) {
		case 0:
			// tuple-sourcenode join (node condition)
		case 1:
			// tuple-destnode join (node condition)
		case 2:
			// tuple-edge join (edge condition)
		default:
			// error
		}
		//NestedJoinNode(arg, condition);
	}
/** Constructor for join of a Tuple and edge or node.
Parameters as in NestedLoopsJoins class
IndexNestedJoin for merge of a previous join and a node/edge; condition on the edge; use typejoin 4-6 (otherwise, pass null argument)
CondExpr for condition on edge/nodes
typejoin as per switch statement below. */
	public IndexNestedJoin(
				AttrType    in1[],    
			   int     len_in1,           
			   short   t1_str_sizes[],
			   AttrType    in2[],         
			   int     len_in2,           
			   short   t2_str_sizes[],   
			   int     amt_of_mem,        
			   Iterator     am1,          
			   String relationName,      
			   FldSpec   proj_list[],
			   int        n_out_flds,
			   IndexNestedJoin arg,
			   CondExpr condition[],
			   int typejoin) 
		throws Exception {
	      _in1 = new AttrType[in1.length];
	      _in2 = new AttrType[in2.length];
	      System.arraycopy(in1,0,_in1,0,in1.length);
	      System.arraycopy(in2,0,_in2,0,in2.length);
	      in1_len = len_in1;
	      in2_len = len_in2;
	      
	      
	      outer = am1;
	      t2_str_sizescopy =  t2_str_sizes;
	      inner_tuple = new Tuple();
	      Jtuple = new Tuple();
	      
	      n_buf_pgs    = amt_of_mem;
	      
	      AttrType[] Jtypes = new AttrType[n_out_flds];
	      short[]    t_size;
	      
	      perm_mat = proj_list;
      	      nOutFlds = n_out_flds;
	      leftArg = arg;

	      try {
		t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
						   in1, len_in1, in2, len_in2,
						   t1_str_sizes, t2_str_sizes,
						   proj_list, nOutFlds);
	      }catch (TupleUtilsException e){
		throw new NestedLoopException(e,"TupleUtilsException is caught by IndexNestedJoin.java");
	      }
	      
	      
	      
	      try {
		  joinHeap = new Heapfile(relationName);
		  
	      }
	      catch(Exception e) {
		throw new NestedLoopException(e, "Create new heapfile failed.");
	      }
		switch(typejoin) {
		case 0:
			sourceEdgeJoin(condition);
			break;
		case 1:
			destEdgeJoin(condition);
			break;
		case 2:
			edgeSourceJoin(condition);
			break;
		case 3:
			edgeDestJoin(condition);
			break;
		case 4: 
			tupleEdgeJoin(condition);
			break; 
		case 5: 
			tupleSourceJoin(condition);
			break;
		case 6: 
			tupleDestJoin(condition);
			break;
		default: 
			status = FAIL;
			close();
		}
	}
	public void sourceEdgeJoin (CondExpr[] edgeCond) 
		throws NestedLoopException, Exception {
		// Node JOIN (source, edge_condition) Edge
		try {
			EdgeScan = SystemDefs.JavabaseDB.getEdgeOpenScan();
	        }
	        catch(Exception e){
		    throw new NestedLoopException(e, "openScan failed on edges");
	        }
		try {
			indexSearch = SystemDefs.JavabaseDB.getSourceScan();
	        }
	        catch(Exception e){
		    throw new NestedLoopException(e, "failed to access source node index");
	        }
		RID rid = new RID();
		Tuple nextEdge = null;
		Tuple nextNode = null;
		while((nextEdge=EdgeScan.getNext(rid)) != null) {
			// Outer loop; check edge condition on all edges in heap
			if(PredEval.Eval(edgeCond, nextEdge, null, _in2, null) == true) {
				while(  (nextNode = 
					SystemDefs.JavabaseDB.getNodeByNID((NID) // get the node by the NID from the heap
					((LeafData) indexSearch.get_next().data).getData()) // Get the NID from the index in the proper format
					) != null) {
					// Inner loop; iterate over all indexed nodes and add the relevant joined tuples to join heap
					Projection.Join(nextNode, _in1,
							nextEdge, _in2,
							Jtuple, perm_mat, nOutFlds);
					joinHeap.insertRecord(Jtuple.getTupleByteArray());
				} 			
			}
		}
		JoinScan = joinHeap.openScan();
	}
	public void destEdgeJoin (CondExpr[] edgeCond) throws NestedLoopException, Exception {
		// Node JOIN (dest, edge_condition) Edge
		try {
			EdgeScan = SystemDefs.JavabaseDB.getEdgeOpenScan();
	        }
	        catch(Exception e){
		    throw new NestedLoopException(e, "openScan failed on edges");
	        }
		try {
			indexSearch = SystemDefs.JavabaseDB.getDestScan();
	        }
	        catch(Exception e){
		    throw new NestedLoopException(e, "failed to access source node index");
	        }
		RID rid = new RID();
		Tuple nextEdge = null;
		Tuple nextNode = null;
		while((nextEdge=EdgeScan.getNext(rid)) != null) {
			// Outer loop; check edge condition on all edges in heap
			if(PredEval.Eval(edgeCond, nextEdge, null, _in2, null) == true) {
				while(  (nextNode = 
					SystemDefs.JavabaseDB.getNodeByNID((NID) // get the node by the NID from the heap
					((LeafData) indexSearch.get_next().data).getData()) // Get the NID from the index in the proper format
					) != null) {
					// Inner loop; iterate over all indexed nodes and add the relevant joined tuples to join heap
					Projection.Join(nextNode, _in1,
							nextEdge, _in2,
							Jtuple, perm_mat, nOutFlds);
					joinHeap.insertRecord(Jtuple.getTupleByteArray());
				} 			
			}
		}
		JoinScan = joinHeap.openScan();
	}
	public void edgeSourceJoin (CondExpr[] nodeCond) throws NestedLoopException, Exception {
		// Edge JOIN (source, node_condition) Node
		try {
			EdgeScan = SystemDefs.JavabaseDB.getEdgeOpenScan();
	        }
	        catch(Exception e){
		    throw new NestedLoopException(e, "openScan failed on edges");
	        }
		try {
			indexSearch = SystemDefs.JavabaseDB.getSourceScan();
	        }
	        catch(Exception e){
		    throw new NestedLoopException(e, "failed to access source node index");
	        }
		RID rid = new RID();
		Tuple nextEdge = null;
		Tuple nextNode = null;
		while((nextEdge=EdgeScan.getNext(rid)) != null) {
			// Outer loop; check edge condition on all edges in heap
			
			while(  (nextNode = 
				SystemDefs.JavabaseDB.getNodeByNID((NID) // get the node by the NID from the heap
				((LeafData) indexSearch.get_next().data).getData()) // Get the NID from the index in the proper format
				) != null) {
				// Inner loop; iterate over all indexed nodes that meet condition and add the relevant joined tuples to join heap
				if(PredEval.Eval(nodeCond, nextNode, null, _in2, null) == true) {
					Projection.Join(nextNode, _in1,
							nextEdge, _in2,
							Jtuple, perm_mat, nOutFlds);
					joinHeap.insertRecord(Jtuple.getTupleByteArray());
					}
			} 			

		}
	}
	public void edgeDestJoin (CondExpr[] nodeCond) throws NestedLoopException, Exception {
		// Edge JOIN (dest, node_condition) Node
				try {
			EdgeScan = SystemDefs.JavabaseDB.getEdgeOpenScan();
	        }
	        catch(Exception e){
		    throw new NestedLoopException(e, "openScan failed on edges");
	        }
		try {
			indexSearch = SystemDefs.JavabaseDB.getDestScan();
	        }
	        catch(Exception e){
		    throw new NestedLoopException(e, "failed to access dest node index");
	        }
		RID rid = new RID();
		Tuple nextEdge = null;
		Tuple nextNode = null;
		while((nextEdge=EdgeScan.getNext(rid)) != null) {
			// Outer loop; check edge condition on all edges in heap
			
			while(  (nextNode = 
				SystemDefs.JavabaseDB.getNodeByNID((NID) // get the node by the NID from the heap
				((LeafData) indexSearch.get_next().data).getData()) // Get the NID from the index in the proper format
				) != null) {
				// Inner loop; iterate over all indexed nodes that meet condition and add the relevant joined tuples to join heap
				if(PredEval.Eval(nodeCond, nextNode, null, _in2, null) == true) {
					Projection.Join(nextNode, _in1,
							nextEdge, _in2,
							Jtuple, perm_mat, nOutFlds);
					joinHeap.insertRecord(Jtuple.getTupleByteArray());
					}
			} 			

		}
	}
	public void tupleDestJoin (CondExpr[] nodeCond) throws NestedLoopException, Exception {
		// Tuple JOIN (dest, node_condition) Node
		try {
			indexSearch = SystemDefs.JavabaseDB.getDestScan();
	        }
	        catch(Exception e){
		    throw new NestedLoopException(e, "failed to access dest node index");
	        }
		RID rid = new RID();
		Tuple nextTuple = null;
		Tuple nextNode = null;
		while((nextTuple=leftArg.get_next()) != null) {
			// Outer loop; check edge condition on all edges in heap
			
			while(  (nextNode = 
				SystemDefs.JavabaseDB.getNodeByNID((NID) // get the node by the NID from the heap
				((LeafData) indexSearch.get_next().data).getData()) // Get the NID from the index in the proper format
				) != null) {
				// Inner loop; iterate over all indexed nodes that meet condition and add the relevant joined tuples to join heap
				if(PredEval.Eval(nodeCond, nextNode, null, _in2, null) == true) {
					Projection.Join(nextTuple, _in1,
							nextNode, _in2,
							Jtuple, perm_mat, nOutFlds);
					joinHeap.insertRecord(Jtuple.getTupleByteArray());
					}
			} 			

		}
	}
	public void tupleSourceJoin (CondExpr[] nodeCond) throws NestedLoopException, Exception {
		// Tuple JOIN (dest, node_condition) Node
		try {
			indexSearch = SystemDefs.JavabaseDB.getSourceScan();
	        }
	        catch(Exception e){
		    throw new NestedLoopException(e, "failed to access source node index");
	        }
		RID rid = new RID();
		Tuple nextTuple = null;
		Tuple nextNode = null;
		while((nextTuple=leftArg.get_next()) != null) {
			// Outer loop; check edge condition on all edges in heap
			
			while(  (nextNode = 
				SystemDefs.JavabaseDB.getNodeByNID((NID) // get the node by the NID from the heap
				((LeafData) indexSearch.get_next().data).getData()) // Get the NID from the index in the proper format
				) != null) {
				// Inner loop; iterate over all indexed nodes that meet condition and add the relevant joined tuples to join heap
				if(PredEval.Eval(nodeCond, nextNode, null, _in2, null) == true) {
					Projection.Join(nextTuple, _in1,
							nextNode, _in2,
							Jtuple, perm_mat, nOutFlds);
					joinHeap.insertRecord(Jtuple.getTupleByteArray());
					}
			} 			

		}
	}
	public void tupleEdgeJoin (CondExpr[] edgeCond) 
		throws NestedLoopException, Exception {
		// Tuple JOIN (source, edge_condition) Edge
		try {
			EdgeScan = SystemDefs.JavabaseDB.getEdgeOpenScan();
	        }
	        catch(Exception e){
		    throw new NestedLoopException(e, "openScan failed on edges");
	        }
		RID rid = new RID();
		Tuple nextEdge = null;
		Tuple nextTuple = null;
		while((nextEdge=EdgeScan.getNext(rid)) != null) {
			// Outer loop; check edge condition on all edges in heap
			if(PredEval.Eval(edgeCond, nextEdge, null, _in2, null) == true) {
				while(  (nextTuple = leftArg.get_next()) != null) {
					// Inner loop; iterate over all indexed nodes and add the relevant joined tuples to join heap
					Projection.Join(nextTuple, _in1,
							nextEdge, _in2,
							Jtuple, perm_mat, nOutFlds);
					joinHeap.insertRecord(Jtuple.getTupleByteArray());
				} 			
			}
		}
		JoinScan = joinHeap.openScan();
	}
/** Get the next tuple in the joined heap. Returns null if none remaining and will then start from beginning. 
The entire join should have been performed by the above methods. This will just get them from 
the heap file. */
    public Tuple get_next()
    throws IOException,
	   JoinsException,
	   IndexException,
	   InvalidTupleSizeException,
	   InvalidTypeException, 
	   PageNotReadException,
	   TupleUtilsException, 
	   PredEvalException,
	   SortException,
	   LowMemException,
	   UnknowAttrType,
	   UnknownKeyTypeException,
	   Exception
    {
	RID rid = new RID();
	Tuple result = JoinScan.getNext(rid);
	if(result == null) {
		JoinScan = joinHeap.openScan(); // create a new scan
	}
	return result;
    } 
/** Close the joined file. */
    public void close() 
	   throws IOException, 
	   JoinsException, 
	   SortException,
	   IndexException{
		System.out.println ("\n"); 
		if (status != OK) {
			//bail out
			System.err.println ("*** Error in closing ");
			Runtime.getRuntime().exit(1);
		}
	}
}
