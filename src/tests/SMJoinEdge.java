package tests;

import global.AttrType;
import heap.Tuple;
import iterator.SortMergeEdge;

public class SMJoinEdge {
	
	public static void performSortMergeJoin() {
		SortMergeEdge sme = new SortMergeEdge("97");
		//SortMergeEdge sme = new SortMergeEdge("");  this is also a variation of sort merge edge join
		AttrType [] jtype = new AttrType[16];
		jtype[0] = new AttrType (AttrType.attrString);
		jtype[1] = new AttrType (AttrType.attrString);
		jtype[2] = new AttrType (AttrType.attrInteger);
		jtype[3] = new AttrType (AttrType.attrInteger);
		jtype[4] = new AttrType (AttrType.attrInteger);
		jtype[5] = new AttrType (AttrType.attrInteger);
		jtype[6] = new AttrType (AttrType.attrString);
		jtype[7] = new AttrType (AttrType.attrString);
		jtype[8] = new AttrType (AttrType.attrInteger);
		jtype[9] = new AttrType (AttrType.attrInteger);
		jtype[10] = new AttrType (AttrType.attrInteger);
		jtype[11] = new AttrType (AttrType.attrInteger);
		jtype[12] = new AttrType (AttrType.attrInteger);
		jtype[13] = new AttrType (AttrType.attrInteger);
		jtype[14] = new AttrType (AttrType.attrString);
		jtype[15] = new AttrType (AttrType.attrString);
		Tuple t = new Tuple();
		t = null;
		while ((t = sme.get_next()) != null) {
			try {
				t.print(jtype);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		sme.close();
	}
	public static void main(String args[])
	{
		performSortMergeJoin();
	}
}
