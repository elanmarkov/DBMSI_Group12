package diskmgr;

import java.io.*;
import java.util.*;

import global.*;
import btree.*;
import zindex.*;
import heap.*;
import diskmgr.*;
import index.IndexException;
import iterator.*;
import iterator.Iterator;

public class PathQueryHandler {
	NodeHeapFile nodes;
	EdgeHeapFile edges;
	BTreeFile nodeLabels;
	ZCurve nodeDesc;
	BTreeFile edgeLabels;
	BTreeFile edgeWeights;
	graphDB db;
	private static int SORTPGNUM = 9;
	public PathQueryHandler(NodeHeapFile nodes, EdgeHeapFile edges, BTreeFile nodeLabels, ZCurve nodeDesc, BTreeFile edgeLabels, BTreeFile edgeWeights, graphDB graphDB) {
		this.nodes = nodes;
		this.edges = edges;
		this.nodeLabels = nodeLabels;
		this.nodeDesc = nodeDesc;
		this.edgeLabels = edgeLabels;
		this.edgeWeights = edgeWeights;
		this.db = db;
		
	}
	
	public boolean pathQuery1a(String exp) throws InvalidTupleSizeException, IOException{
		boolean result = false;
        String[] exp1 = exp.split("/");
        System.out.print("hello");
        int sizeOfExp = exp1.length;
        ExpType [] expType = new ExpType[sizeOfExp];
        String[] expValue = new String[sizeOfExp];
        for(int i=0; i<sizeOfExp; i++){
        	if (exp1[i].startsWith("ND")){
        		System.out.print("hello");
        		String[] y = exp1[i].split("ND");
        		expValue[i] = y[1];
        		expType[i] = new ExpType (ExpType.expDesc);
        	}
        	
        	else {
        		System.out.print("hello");
        		exp1[i].split("NL");
        		String[] y = exp1[i].split("NL");
        		expValue[i] = y[1];
        		expType[i] = new ExpType (ExpType.expNodeLabel);
        	}
      
        }
        
        PathExpression pe = SystemDefs.JavabaseDB.getPathExpressions();
        AttrType[] Jtypes = { new AttrType(AttrType.attrString), 
				new AttrType(AttrType.attrString),
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrString), 
				new AttrType(AttrType.attrString), };
        short[] attrSize = new short[4];
        attrSize[0] = Tuple.LABEL_MAX_LENGTH;
        attrSize[1] = Tuple.LABEL_MAX_LENGTH;
        attrSize[2] = 4;
        attrSize[3] = 4;
        
        Iterator it = pe.evaluatePathExpression(expType, expValue);
        try {
        	System.out.print("hello");
			Tuple t =null;
			while ((t = it.get_next()) != null) {
				t.print(Jtypes);
			}
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
        try {
			pe.close();
		} catch (JoinsException | SortException | IndexException e) {
			e.printStackTrace();
		} 
        
        
        return result;
	}
	
	public boolean pathQuery1b(String exp) throws InvalidTupleSizeException, IOException{
		boolean result = false;
        String[] exp1 = exp.split("/");
        
        int sizeOfExp = exp1.length;
        ExpType [] expType = new ExpType[sizeOfExp];
        String[] expValue = new String[sizeOfExp];
        for(int i=0; i<sizeOfExp; i++){
        	if (exp1[i].startsWith("ND")){
        		System.out.print("hello");
        		String[] y = exp1[i].split("ND");
        		expValue[i] = y[1];
        		expType[i] = new ExpType (ExpType.expDesc);
        	}
        	else {
        		System.out.print("hello");
        		exp1[i].split("NL");
        		String[] y = exp1[i].split("NL");
        		expValue[i] = y[1];
        		expType[i] = new ExpType (ExpType.expNodeLabel);
        	}
        }
        
        PathExpression pe = SystemDefs.JavabaseDB.getPathExpressions();
        AttrType[] Jtypes = { new AttrType(AttrType.attrString), 
				new AttrType(AttrType.attrString),
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrString), 
				new AttrType(AttrType.attrString), };
        short[] attrSize = new short[4];
        attrSize[0] = Tuple.LABEL_MAX_LENGTH;
        attrSize[1] = Tuple.LABEL_MAX_LENGTH;
        attrSize[2] = 4;
        attrSize[3] = 4;
        
        Iterator it = pe.evaluatePathExpression(expType, expValue);
      
			try {
				Sort sort = new Sort(Jtypes, (short) 5, attrSize, it, 4,
						new TupleOrder(TupleOrder.Ascending), 
						4, SORTPGNUM);
				Tuple t =null;
				while ((t = sort.get_next()) != null){
					t.print(Jtypes);
					//t=sort.get_next();
				}
				it.close();
				sort.close();

			} catch (Exception e) {
			
				e.printStackTrace();
			}
			try {
				pe.close();
			} catch (JoinsException | SortException | IndexException e) {
				e.printStackTrace();
			} 
		
        
        return result;
	}
	
	public boolean pathQuery1c(String exp) throws InvalidTupleSizeException, IOException{
		boolean result = false;
		return result;
	}
	public boolean pathQuery2a(String exp) throws InvalidTupleSizeException, IOException{
		boolean result = false;
        String[] exp1 = exp.split("/");
        
        int sizeOfExp = exp1.length;
        ExpType [] expType = new ExpType[sizeOfExp];
        String[] expValue = new String[sizeOfExp];
        for(int i=0; i<sizeOfExp; i++){
        	if (exp1[i].startsWith("ND")){
        		System.out.print("hello");
        		String[] y = exp1[i].split("ND");
        		expValue[i] = y[1];
        		expType[i] = new ExpType (ExpType.expDesc);
        	}
        	else if(exp1[i].startsWith("EL")){
        		System.out.print("hello");
        		exp1[i].split("EL");
        		String[] y = exp1[i].split("EL");
        		expValue[i] = y[1];
        		expType[i] = new ExpType (ExpType.expEdgeLabel);
        	}
        	else if(exp1[i].startsWith("NL")){
        		System.out.print("hello");
        		exp1[i].split("NL");
        		String[] y = exp1[i].split("NL");
        		expValue[i] = y[1];
        		expType[i] = new ExpType (ExpType.expNodeLabel);
        	}
        	else if(exp1[i].startsWith("MW")){
        		System.out.print("hello");
        		exp1[i].split("MW");
        		String[] y = exp1[i].split("MW");
        		expValue[i] = y[1];
        		expType[i] = new ExpType (ExpType.expWeight);
        	}
        }
        
        PathExpression pe = SystemDefs.JavabaseDB.getPathExpressions();
        AttrType[] Jtypes = { new AttrType(AttrType.attrString), 
				new AttrType(AttrType.attrString),
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrString), 
				new AttrType(AttrType.attrString), };
        short[] attrSize = new short[4];
        attrSize[0] = Tuple.LABEL_MAX_LENGTH;
        attrSize[1] = Tuple.LABEL_MAX_LENGTH;
        attrSize[2] = 4;
        attrSize[3] = 4;
        
        Iterator it = pe.evaluatePathExpression(expType, expValue);
        try {
        	System.out.print("hello");
			Tuple t =null;
			while ((t = it.get_next()) != null) {
				t.print(Jtypes);
			}
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
        try {
			pe.close();
		} catch (JoinsException | SortException | IndexException e) {
			e.printStackTrace();
		} 
        
        
        return result;
	}
	
	
	public boolean pathQuery2b(String exp) throws InvalidTupleSizeException, IOException{
		boolean result = false;
        String[] exp1 = exp.split("/");
        
        int sizeOfExp = exp1.length;
        ExpType [] expType = new ExpType[sizeOfExp];
        String[] expValue = new String[sizeOfExp];
        for(int i=0; i<sizeOfExp; i++){
        	if (exp1[i].startsWith("ND")){
        		System.out.print("hello");
        		String[] y = exp1[i].split("ND");
        		expValue[i] = y[1];
        		expType[i] = new ExpType (ExpType.expDesc);
        	}
        	else if(exp1[i].startsWith("EL")){
        		System.out.print("hello");
        		exp1[i].split("EL");
        		String[] y = exp1[i].split("EL");
        		expValue[i] = y[1];
        		expType[i] = new ExpType (ExpType.expEdgeLabel);
        	}
        	else if(exp1[i].startsWith("NL")){
        		System.out.print("hello");
        		exp1[i].split("NL");
        		String[] y = exp1[i].split("NL");
        		expValue[i] = y[1];
        		expType[i] = new ExpType (ExpType.expNodeLabel);
        	}
        	else if(exp1[i].startsWith("MW")){
        		System.out.print("hello");
        		exp1[i].split("MW");
        		String[] y = exp1[i].split("MW");
        		expValue[i] = y[1];
        		expType[i] = new ExpType (ExpType.expWeight);
        	}
        }
        
        PathExpression pe = SystemDefs.JavabaseDB.getPathExpressions();
        AttrType[] Jtypes = { new AttrType(AttrType.attrString), 
				new AttrType(AttrType.attrString),
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrString), 
				new AttrType(AttrType.attrString), };
        short[] attrSize = new short[4];
        attrSize[0] = Tuple.LABEL_MAX_LENGTH;
        attrSize[1] = Tuple.LABEL_MAX_LENGTH;
        attrSize[2] = 4;
        attrSize[3] = 4;
        
        Iterator it = pe.evaluatePathExpression(expType, expValue);
        
			try {
				Sort sort = new Sort(Jtypes, (short) 5, attrSize, it, 5,
						new TupleOrder(TupleOrder.Ascending), 
						Tuple.LABEL_MAX_LENGTH, SORTPGNUM);
				Tuple t =null;
				while ((t = sort.get_next()) != null){
					t.print(Jtypes);
				}
				it.close();
				sort.close();

			} catch (Exception e) {
			
				e.printStackTrace();
			}
			try {
				pe.close();
			} catch (JoinsException | SortException | IndexException e) {
				e.printStackTrace();
			} 
		
        
        return result;
	}
	
	public boolean pathQuery2c(String exp){
		return true;
	}
	
	public boolean pathQuery3a(String exp){
		return true;
	}

	public boolean pathQuery3b(String exp){
		return true;
	}
	

	
	public boolean pathQuery3c(String exp){
		return true;
	}
}
