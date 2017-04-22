	package iterator;

	import heap.*;
	import global.*;
	import bufmgr.*;
	import diskmgr.*;
	import index.*;
	import java.lang.*;
	import java.io.*;

public class NestedIndexLoopJoin extends Iterator{

		private static int count = 0;
		private AttrType _in1[], _in2[];
		private int in1_len, in2_len;
		private Iterator outer;
		private short t2_str_sizescopy[];
		private CondExpr OutputFilter[];
		private CondExpr RightFilter[];
		private int n_buf_pgs; // # of buffer pages available.
		private boolean done, // Is the join complete
				get_from_outer; // if TRUE, a tuple is got from outer
		private Tuple outer_tuple, inner_tuple;
		private Tuple Jtuple; // Joined tuple
		private FldSpec perm_mat[];
		private int nOutFlds;
		private Heapfile hf;
		private IndexScan inner;
		private String resultsFileName;
		private int indexFldNo;
		private String indexName;
		private String indexRelName;
		private FldSpec indexProjList[];
		private IndexType indexType;
		private int joinFldNo;
		
		public String getFileName() {
			return resultsFileName;
		}

		/**
		 * constructor Initialize the two relations which are joined, including
		 * relation type,
		 * 
		 * @param in1
		 *            Array containing field types of R.
		 * @param len_in1
		 *            # of columns in R.
		 * @param t1_str_sizes
		 *            shows the length of the string fields.
		 * @param in2
		 *            Array containing field types of S
		 * @param len_in2
		 *            # of columns in S
		 * @param t2_str_sizes
		 *            shows the length of the string fields.
		 * @param amt_of_mem
		 *            IN PAGES
		 * @param am1
		 *            access method for left i/p to join
		 * @param relationName
		 *            access hfapfile for right i/p to join
		 * @param outFilter
		 *            select expressions
		 * @param rightFilter
		 *            reference to filter applied on right i/p
		 * @param proj_list
		 *            shows what input fields go where in the output tuple
		 * @param n_out_flds
		 *            number of outer relation fileds
		 * @exception IOException
		 *                some I/O fault
		 * @exception NestedLoopException
		 *                exception from this class
		 */
		public NestedIndexLoopJoin(AttrType in1[], int len_in1, short t1_str_sizes[], AttrType in2[], int len_in2,
				short t2_str_sizes[], int amt_of_mem, Iterator am1, String indexName, IndexType itype, String indexedRelName,FldSpec indexProjList[],
				int indexFldNo, CondExpr outFilter[], CondExpr rightFilter[], FldSpec proj_list[], int n_out_flds, int joinFldNo) throws IOException, NestedLoopException {

			_in1 = new AttrType[in1.length];
			_in2 = new AttrType[in2.length];
			System.arraycopy(in1, 0, _in1, 0, in1.length);
			System.arraycopy(in2, 0, _in2, 0, in2.length);
			in1_len = len_in1;
			in2_len = len_in2;

			outer = am1;
			t2_str_sizescopy = t2_str_sizes;
			inner_tuple = new Tuple();
			Jtuple = new Tuple();
			OutputFilter = outFilter;
			RightFilter = rightFilter;

			n_buf_pgs = amt_of_mem;
			inner = null;
			done = false;
			get_from_outer = true;
			this.joinFldNo = joinFldNo;
			AttrType[] Jtypes = new AttrType[n_out_flds];
			short[] t_size;

			perm_mat = proj_list;
			nOutFlds = n_out_flds;
			this.indexFldNo=indexFldNo;
			this.indexName=indexName;
			this.indexRelName=indexedRelName;
			this.indexProjList=indexProjList;
			this.indexType = itype;
			
			try {
				t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes, in1, len_in1, in2, len_in2, t1_str_sizes, t2_str_sizes,
						proj_list, nOutFlds);
			} catch (TupleUtilsException e) {
				throw new NestedLoopException(e, "TupleUtilsException is caught by NestedLoopsJoins.java");
			}

			try {
				inner = new IndexScan(itype, indexedRelName, indexName, in2, t2_str_sizes, in2.length, in2.length, indexProjList, rightFilter, indexFldNo, false);
			} catch (Exception e) {
				throw new NestedLoopException(e, "Create new heapfile failed.");
			}
		}

		/**
		 * @return The joined tuple is returned
		 * @exception IOException
		 *                I/O errors
		 * @exception JoinsException
		 *                some join exception
		 * @exception IndexException
		 *                exception from super class
		 * @exception InvalidTupleSizeException
		 *                invalid tuple size
		 * @exception InvalidTypeException
		 *                tuple type not valid
		 * @exception PageNotReadException
		 *                exception from lower layer
		 * @exception TupleUtilsException
		 *                exception from using tuple utilities
		 * @exception PredEvalException
		 *                exception from PredEval class
		 * @exception SortException
		 *                sort exception
		 * @exception LowMemException
		 *                memory error
		 * @exception UnknowAttrType
		 *                attribute type unknown
		 * @exception UnknownKeyTypeException
		 *                key type unknown
		 * @exception Exception
		 *                other exceptions
		 * 
		 */
		public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
				InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
				LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
			// This is a DUMBEST form of a join, not making use of any key
			// information...
			//int count =0;
			if (done)
				return null;

			do {
				// If get_from_outer is true, Get a tuple from the outer, delete
				// an existing scan on the file, and reopen a new scan on the file.
				// If a get_next on the outer returns DONE?, then the nested loops
				// join is done too.
				
				if (get_from_outer == true) {
					get_from_outer = false;
					if (inner != null) // If this not the first time,
					{
						// close scan
						inner.close();
						inner = null;
					}

					
					if ((outer_tuple = outer.get_next()) == null) {
						done = true;
						/*if (inner != null) {
							inner.close();
							inner = null;
						}*/
						return null;
					}
					String label = outer_tuple.getStrFld(joinFldNo);
					CondExpr[] indexExpr = new CondExpr[2];
					indexExpr[0] = new CondExpr();
					indexExpr[0].op = new AttrOperator(AttrOperator.aopEQ);
					indexExpr[0].type1 = new AttrType(AttrType.attrSymbol);
					indexExpr[0].type2 = new AttrType(AttrType.attrString);
					indexExpr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), indexFldNo);
					indexExpr[0].operand2.string = label;
					indexExpr[0].next = null;
				    indexExpr[1] = null;
					try {
						inner = new IndexScan(this.indexType, this.indexRelName, this.indexName, _in2, t2_str_sizescopy, _in2.length, _in2.length, indexProjList, indexExpr, indexFldNo, false);
					} catch (Exception e) {
						throw new NestedLoopException(e, "openScan failed");
					}
				} // ENDS: if (get_from_outer == TRUE)

				// The next step is to get a tuple from the inner,
				// while the inner is not completely scanned && there
				// is no match (with pred),get a tuple from the inner.
				//System.out.println("NestedIndexLoopJoin.get_next() got outer tupple "+outer_tuple);
				//outer_tuple.print(_in1);
				while ((inner_tuple = inner.get_next()) != null) {
					/*System.out.println("NestedIndexLoopJoin.get_next() got outer tuple");
					outer_tuple.print(_in1);
					System.out.println("NestedIndexLoopJoin.get_next() inner tuple");
					inner_tuple.print(_in2);*/
					inner_tuple.setHdr((short) in2_len, _in2, t2_str_sizescopy);
					if (PredEval.Eval(RightFilter, inner_tuple, null, _in2, null) == true) {
						if (PredEval.Eval(OutputFilter, outer_tuple, inner_tuple, _in1, _in2) == true) {
							// Apply a projection on the outer and inner tuples.
							Projection.Join(outer_tuple, _in1, inner_tuple, _in2, Jtuple, perm_mat, nOutFlds);
							return Jtuple;
						}
					}
				}

				// There has been no match. (otherwise, we would have
				// returned from t//he while loop. Hence, inner is
				// exhausted, => set get_from_outer = TRUE, go to top of loop

				get_from_outer = true; // Loop back to top and get next outer tuple.
			} while (true);
		}

		/**
		 * implement the abstract method close() from super class Iterator to finish
		 * cleaning up
		 * 
		 * @exception IOException
		 *                I/O error from lower layers
		 * @exception JoinsException
		 *                join error from lower layers
		 * @exception IndexException
		 *                index access error
		 */
		public void close() throws JoinsException, IOException, IndexException {
			if (!closeFlag) {

				try {
					outer.close();
				} catch (Exception e) {
					throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
				}
				closeFlag = true;
			}
		}
	}
