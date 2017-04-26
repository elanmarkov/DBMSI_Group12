package diskmgr;

import java.io.*;
import java.util.*;

import com.sun.org.apache.bcel.internal.generic.ArrayType;

import global.*;
import btree.*;
import zindex.*;
import heap.*;
import diskmgr.*;
import index.IndexException;
import iterator.*;
import iterator.Iterator;

public class PathQueryHandler {
        private static final String PQ_INTERMEDIATE_FILENAME = "pqIntermediate";
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

        int sizeOfExp = exp1.length;
        ExpType [] expType = new ExpType[sizeOfExp];
        String[] expValue = new String[sizeOfExp];
        for(int i=0; i<sizeOfExp; i++){
                if (exp1[i].startsWith("ND")){
                        String[] y = exp1[i].split("ND");
                        expValue[i] = y[1];
                        expType[i] = new ExpType (ExpType.expDesc);
                }
                
                else {
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
                                new AttrType(AttrType.attrString)};
        int[] fldsArray = {1,5};
        
        Iterator it = pe.evaluatePathExpression(expType, expValue);
        try {
                        Tuple t =null;
                        while ((t = it.get_next()) != null) {
                                t.print(Jtypes, fldsArray);
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
                        String[] y = exp1[i].split("ND");
                        expValue[i] = y[1];
                        expType[i] = new ExpType (ExpType.expDesc);
                }
                else {
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
                                new AttrType(AttrType.attrString),};
        short[] attrSize = new short[4];
        attrSize[0] = Tuple.LABEL_MAX_LENGTH;
        attrSize[1] = Tuple.LABEL_MAX_LENGTH;
        attrSize[2] = 4;
        attrSize[3] = 4;
        int[] fldsArray = {1,5};
        
        Iterator it = pe.evaluatePathExpression(expType, expValue);
      
                        try {
                                Sort sort = new Sort(Jtypes, (short) 5, attrSize, it, 4,
                                                new TupleOrder(TupleOrder.Ascending), 
                                                4, SORTPGNUM);
                                Tuple t =null;
                                while ((t = sort.get_next()) != null){
                                        t.print(Jtypes, fldsArray);
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
        String[] exp1 = exp.split("/");
        
        int sizeOfExp = exp1.length;
        ExpType [] expType = new ExpType[sizeOfExp];
        String[] expValue = new String[sizeOfExp];
        for(int i=0; i<sizeOfExp; i++){
                if (exp1[i].startsWith("ND")){
                        String[] y = exp1[i].split("ND");
                        expValue[i] = y[1];
                        expType[i] = new ExpType (ExpType.expDesc);
                }
                else {
                        exp1[i].split("NL");
                        String[] y = exp1[i].split("NL");
                        expValue[i] = y[1];
                        expType[i] = new ExpType (ExpType.expNodeLabel);
                }
        }
        
        PathExpression pe = SystemDefs.JavabaseDB.getPathExpressions();
        NodeHeapFile intermediateFile=null;
                AttrType[] Jtypes = { new AttrType(AttrType.attrString), 
                                new AttrType(AttrType.attrString)};
        AttrType[] newJtype = {new AttrType(AttrType.attrString), 
                                new AttrType(AttrType.attrString), new AttrType(AttrType.attrString)};
        short[] Jsizes = new short[4];
                Jsizes[0] = 32;
                Jsizes[1] = Tuple.LABEL_MAX_LENGTH;
                Jsizes[2] = 4;
                
                FldSpec[] JProjList = new FldSpec[3];
                RelSpec relation = new RelSpec(RelSpec.outer);
                JProjList[0] = new FldSpec(relation, 1);
                JProjList[1] = new FldSpec(relation, 2);
                JProjList[2] = new FldSpec(relation, 3);

                CondExpr[] jFilter = null;
                
                int  lableMaxLength = Tuple.LABEL_MAX_LENGTH;
        FileScan fam = null;
        Iterator it = pe.evaluatePathExpression(expType, expValue);
        
        try {
                        Tuple t =null;
                        intermediateFile = new NodeHeapFile(PQ_INTERMEDIATE_FILENAME);
                        while ((t = it.get_next()) != null){
                                Tuple tp = new Tuple();
                                String firstFld = t.getStrFld(1);
                                String secondFld = t.getStrFld(5);
                                String concatFld = firstFld+secondFld;
                                tp.setHdr((short)3, newJtype, Jsizes);
                                tp.setStrFld(1, concatFld);
                                tp.setStrFld(2, firstFld);
                                tp.setStrFld(3, secondFld);
                                intermediateFile.insertNode(tp.getTupleByteArray());
                        }
                } catch (Exception e) {
                        e.printStackTrace();
                }
        try {
                fam = new FileScan(PQ_INTERMEDIATE_FILENAME,newJtype,
                        Jsizes, (short) newJtype.length, (short)newJtype.length, JProjList, null);
                        Tuple t =null;
                        int[] newfldsArray = {2,3};
                        DuplElim dup = new DuplElim(newJtype, (short)newJtype.length, Jsizes, fam, 10, false);
                        while ((t = dup.get_next()) != null){
                                t.print(newJtype, newfldsArray);
                        }
                        fam.close();
                        intermediateFile.deleteFile();
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
        public boolean pathQuery2a(String exp) throws InvalidTupleSizeException, IOException{
                boolean result = false;
        String[] exp1 = exp.split("/");
        
        int sizeOfExp = exp1.length;
        ExpType [] expType = new ExpType[sizeOfExp];
        String[] expValue = new String[sizeOfExp];
        for(int i=0; i<sizeOfExp; i++){
                if (exp1[i].startsWith("ND")){
                        String[] y = exp1[i].split("ND");
                        expValue[i] = y[1];
                        expType[i] = new ExpType (ExpType.expDesc);
                }
                else if(exp1[i].startsWith("EL")){
                        exp1[i].split("EL");
                        String[] y = exp1[i].split("EL");
                        expValue[i] = y[1];
                        expType[i] = new ExpType (ExpType.expEdgeLabel);
                }
                else if(exp1[i].startsWith("NL")){
                        exp1[i].split("NL");
                        String[] y = exp1[i].split("NL");
                        expValue[i] = y[1];
                        expType[i] = new ExpType (ExpType.expNodeLabel);
                }
                else if(exp1[i].startsWith("MW")){
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
        int[] fldsArray = {1,5};
        
        Iterator it = pe.evaluatePathExpression(expType, expValue);
        try {
                        Tuple t =null;
                        while ((t = it.get_next()) != null) {
                                t.print(Jtypes, fldsArray);
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
                        String[] y = exp1[i].split("ND");
                        expValue[i] = y[1];
                        expType[i] = new ExpType (ExpType.expDesc);
                }
                else if(exp1[i].startsWith("EL")){
                        exp1[i].split("EL");
                        String[] y = exp1[i].split("EL");
                        expValue[i] = y[1];
                        expType[i] = new ExpType (ExpType.expEdgeLabel);
                }
                else if(exp1[i].startsWith("NL")){
                        exp1[i].split("NL");
                        String[] y = exp1[i].split("NL");
                        expValue[i] = y[1];
                        expType[i] = new ExpType (ExpType.expNodeLabel);
                }
                else if(exp1[i].startsWith("MW")){
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
                                new AttrType(AttrType.attrString),};
        short[] attrSize = new short[4];
        attrSize[0] = Tuple.LABEL_MAX_LENGTH;
        attrSize[1] = Tuple.LABEL_MAX_LENGTH;
        attrSize[2] = 4;
        attrSize[3] = 4;
        int[] fldsArray  = {1,5};
        
        Iterator it = pe.evaluatePathExpression(expType, expValue);
        
                        try {
                                Sort sort = new Sort(Jtypes, (short) 5, attrSize, it, 5,
                                                new TupleOrder(TupleOrder.Ascending), 
                                                Tuple.LABEL_MAX_LENGTH, SORTPGNUM);
                                Tuple t =null;
                                while ((t = sort.get_next()) != null){
                                        t.print(Jtypes, fldsArray);
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
        
        public boolean pathQuery2c(String exp) throws IOException{
                boolean result = false;
        String[] exp1 = exp.split("/");
        
        int sizeOfExp = exp1.length;
        ExpType [] expType = new ExpType[sizeOfExp];
        String[] expValue = new String[sizeOfExp];
        for(int i=0; i<sizeOfExp; i++){
                if (exp1[i].startsWith("ND")){
                        String[] y = exp1[i].split("ND");
                        expValue[i] = y[1];
                        expType[i] = new ExpType (ExpType.expDesc);
                }
                else if(exp1[i].startsWith("EL")){
                        exp1[i].split("EL");
                        String[] y = exp1[i].split("EL");
                        expValue[i] = y[1];
                        expType[i] = new ExpType (ExpType.expEdgeLabel);
                }
                else if(exp1[i].startsWith("NL")){
                        exp1[i].split("NL");
                        String[] y = exp1[i].split("NL");
                        expValue[i] = y[1];
                        expType[i] = new ExpType (ExpType.expNodeLabel);
                }
                else if(exp1[i].startsWith("MW")){
                        exp1[i].split("MW");
                        String[] y = exp1[i].split("MW");
                        expValue[i] = y[1];
                        expType[i] = new ExpType (ExpType.expWeight);
                }
        }        
        
        PathExpression pe = SystemDefs.JavabaseDB.getPathExpressions();
        NodeHeapFile intermediateFile=null;
                AttrType[] Jtypes = { new AttrType(AttrType.attrString), 
                                new AttrType(AttrType.attrString)};
        AttrType[] newJtype = {new AttrType(AttrType.attrString), 
                                new AttrType(AttrType.attrString), new AttrType(AttrType.attrString)};
        short[] Jsizes = new short[4];
                Jsizes[0] = 32;
                Jsizes[1] = Tuple.LABEL_MAX_LENGTH;
                Jsizes[2] = 4;
                
                FldSpec[] JProjList = new FldSpec[3];
                RelSpec relation = new RelSpec(RelSpec.outer);
                JProjList[0] = new FldSpec(relation, 1);
                JProjList[1] = new FldSpec(relation, 2);
                JProjList[2] = new FldSpec(relation, 3);

                CondExpr[] jFilter = null;
                
                int  lableMaxLength = Tuple.LABEL_MAX_LENGTH;
        short[] attrSize = new short[4];
        attrSize[0] = Tuple.LABEL_MAX_LENGTH;
        attrSize[1] = Tuple.LABEL_MAX_LENGTH;
        attrSize[2] = 4;
        attrSize[3] = 4;
        int[] fldsArray = {1,5};
        FileScan fam = null;
        Iterator it = pe.evaluatePathExpression(expType, expValue);
        
        try {
                        Tuple t =null;
                        intermediateFile = new NodeHeapFile(PQ_INTERMEDIATE_FILENAME);
                        while ((t = it.get_next()) != null){
                                Tuple tp = new Tuple();
                                String firstFld = t.getStrFld(1);
                                String secondFld = t.getStrFld(5);
                                String concatFld = firstFld+secondFld;
                                tp.setHdr((short)3, newJtype, Jsizes);
                                tp.setStrFld(1, concatFld);
                                tp.setStrFld(2, firstFld);
                                tp.setStrFld(3, secondFld);
                                intermediateFile.insertNode(tp.getTupleByteArray());
                        }
                } catch (Exception e) {
                        e.printStackTrace();
                }
        try {
                fam = new FileScan(PQ_INTERMEDIATE_FILENAME,newJtype,
                        Jsizes, (short) newJtype.length, (short)newJtype.length, JProjList, null);
                        Tuple t =null;
                        int[] newfldsArray = {2,3};
                        DuplElim dup = new DuplElim(newJtype, (short)newJtype.length, Jsizes, fam, 10, false);
                        while ((t = dup.get_next()) != null){
                                t.print(newJtype, newfldsArray);
                        }
                        fam.close();
                        intermediateFile.deleteFile();
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
        
        public boolean pathQuery3a(String exp) throws IOException{
                boolean result = false;
        String[] exp1 = exp.split("/");
        int bound;
        ExpType boundType;
        
        int sizeOfExp = exp1.length;
        ExpType expType;
        String expValue = "";
        
                if (exp1[0].startsWith("ND")){
                        String[] y = exp1[0].split("ND");
                        expValue = y[1];
                        expType = new ExpType(ExpType.expDesc);
                }
                else if(exp1[0].startsWith("NL")){
                        String[] y = exp1[0].split("NL");
                        expValue = y[1];
                        expType = new ExpType (ExpType.expNodeLabel);
                }
                else
                {
                        System.out.println("INVALID PATH EXPRESSION");
                        return false;
                }
                
                if(exp1[1].startsWith("ME")){
                        exp1[1].split("ME");
                        bound = Integer.parseInt(exp1[1].split("ME")[1]);
                        boundType = new ExpType(ExpType.expNoOfEdges);
                }
                else if(exp1[1].startsWith("TW")){
                        exp1[1].split("TW");
                        bound = Integer.parseInt(exp1[1].split("TW")[1]);
                        boundType = new ExpType(ExpType.expTotalWeights );
                }
                else
                {
                        System.out.println("INVALID PATH EXPRESSION");
                        return false;
                }
        
        
        PathExpression pe = SystemDefs.JavabaseDB.getPathExpressions();
        AttrType[] Jtypes = { new AttrType(AttrType.attrString), 
                                new AttrType(AttrType.attrString),
                                new AttrType(AttrType.attrInteger),
                                new AttrType(AttrType.attrString), 
                                new AttrType(AttrType.attrString),
                                new AttrType(AttrType.attrInteger),};
        short[] attrSize = new short[4];
        attrSize[0] = Tuple.LABEL_MAX_LENGTH;
        attrSize[1] = Tuple.LABEL_MAX_LENGTH;
        attrSize[2] = 4;
        attrSize[3] = 4;
        int[] fldsArray = {1,5};
        
        Iterator it = pe.evaluateBoundPathExpression(expType, expValue, boundType, bound);
        
        try {
                        Tuple t =null;
                        while ((t = it.get_next()) != null) {
                                t.print(Jtypes, fldsArray);
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

        public boolean pathQuery3b(String exp) throws IOException{
                boolean result = false;
        String[] exp1 = exp.split("/");
        int bound;
        ExpType boundType;
        
        int sizeOfExp = exp1.length;
        ExpType expType;
        String expValue = "";
        
                if (exp1[0].startsWith("ND")){
                        String[] y = exp1[0].split("ND");
                        expValue = y[1];
                        expType = new ExpType(ExpType.expDesc);
                }
                else if(exp1[0].startsWith("NL")){
                        String[] y = exp1[0].split("NL");
                        expValue = y[1];
                        expType = new ExpType (ExpType.expNodeLabel);
                }
                else
                {
                        System.out.println("INVALID PATH EXPRESSION");
                        return false;
                }
                
                if(exp1[1].startsWith("ME")){
                        exp1[1].split("ME");
                        bound = Integer.parseInt(exp1[1].split("ME")[1]);
                        boundType = new ExpType(ExpType.expNoOfEdges);
                }
                else if(exp1[1].startsWith("TW")){
                        exp1[1].split("TW");
                        bound = Integer.parseInt(exp1[1].split("TW")[1]);
                        boundType = new ExpType(ExpType.expTotalWeights );
                }
                else
                {
                        System.out.println("INVALID PATH EXPRESSION");
                        return false;
                }
        
                
        
        PathExpression pe = SystemDefs.JavabaseDB.getPathExpressions();
        AttrType[] Jtypes =null;
        if(exp1[1].startsWith("ME")){
        	Jtypes =new AttrType[5];
        	Jtypes[0] =  new AttrType(AttrType.attrString); 
        	Jtypes[1] =  new AttrType(AttrType.attrString);
        	Jtypes[2] =  new AttrType(AttrType.attrInteger);
        	Jtypes[3] =  new AttrType(AttrType.attrString);
        	Jtypes[4] =  new AttrType(AttrType.attrString);
        }
        else{
        	// if tw
        	Jtypes =new AttrType[6];
        	Jtypes[0] =  new AttrType(AttrType.attrString); 
        	Jtypes[1] =  new AttrType(AttrType.attrString);
        	Jtypes[2] =  new AttrType(AttrType.attrInteger);
        	Jtypes[3] =  new AttrType(AttrType.attrString);
        	Jtypes[4] =  new AttrType(AttrType.attrString);
        	Jtypes[5] =  new AttrType(AttrType.attrInteger);
    	}
        
        short[] attrSize = new short[4];
        attrSize[0] = Tuple.LABEL_MAX_LENGTH;
        attrSize[1] = Tuple.LABEL_MAX_LENGTH;
        attrSize[2] = 4;
        attrSize[3] = 4;
        int[] fldsArray = {1,5};
        
        Iterator it = pe.evaluateBoundPathExpression(expType, expValue, boundType, bound);
        
        try {
                        Sort sort = new Sort(Jtypes, (short) Jtypes.length, attrSize, it, 5,
                                        new TupleOrder(TupleOrder.Ascending), 
                                        4, SORTPGNUM);
                        Tuple t =null;
                        while ((t = sort.get_next()) != null){
                        		t.print(Jtypes, fldsArray);
                        }
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
        

        
        public boolean pathQuery3c(String exp) throws IOException{
                boolean result = false;
        String[] exp1 = exp.split("/");
        int bound;
        ExpType boundType;
        
        int sizeOfExp = exp1.length;
        ExpType expType;
        String expValue = "";
        
                if (exp1[0].startsWith("ND")){
                        String[] y = exp1[0].split("ND");
                        expValue = y[1];
                        expType = new ExpType(ExpType.expDesc);
                }
                else if(exp1[0].startsWith("NL")){
                        String[] y = exp1[0].split("NL");
                        expValue = y[1];
                        expType = new ExpType (ExpType.expNodeLabel);
                }
                else
                {
                        System.out.println("INVALID PATH EXPRESSION");
                        return false;
                }
                
                if(exp1[1].startsWith("ME")){
                        exp1[1].split("ME");
                        bound = Integer.parseInt(exp1[1].split("ME")[1]);
                        boundType = new ExpType(ExpType.expNoOfEdges);
                }
                else if(exp1[1].startsWith("TW")){
                        exp1[1].split("TW");
                        bound = Integer.parseInt(exp1[1].split("TW")[1]);
                        boundType = new ExpType(ExpType.expTotalWeights );
                }
                else
                {
                        System.out.println("INVALID PATH EXPRESSION");
                        return false;
                }
        
        
        PathExpression pe = SystemDefs.JavabaseDB.getPathExpressions();
        NodeHeapFile intermediateFile=null;
                AttrType[] Jtypes = { new AttrType(AttrType.attrString), 
                                new AttrType(AttrType.attrString)};
        AttrType[] newJtype = {new AttrType(AttrType.attrString), 
                                new AttrType(AttrType.attrString), new AttrType(AttrType.attrString)};
        short[] Jsizes = new short[4];
                Jsizes[0] = 32;
                Jsizes[1] = Tuple.LABEL_MAX_LENGTH;
                Jsizes[2] = 4;
                
                FldSpec[] JProjList = new FldSpec[3];
                RelSpec relation = new RelSpec(RelSpec.outer);
                JProjList[0] = new FldSpec(relation, 1);
                JProjList[1] = new FldSpec(relation, 2);
                JProjList[2] = new FldSpec(relation, 3);

                CondExpr[] jFilter = null;
                
                int  lableMaxLength = Tuple.LABEL_MAX_LENGTH;
        short[] attrSize = new short[4];
        attrSize[0] = Tuple.LABEL_MAX_LENGTH;
        attrSize[1] = Tuple.LABEL_MAX_LENGTH;
        attrSize[2] = 4;
        attrSize[3] = 4;
        int[] fldsArray = {1,5};
        FileScan fam = null;
        Iterator it = pe.evaluateBoundPathExpression(expType, expValue, boundType, bound);
        
        try {
                        Tuple t =null;
                        intermediateFile = new NodeHeapFile(PQ_INTERMEDIATE_FILENAME);
                        while ((t = it.get_next()) != null){
                                Tuple tp = new Tuple();
                                String firstFld = t.getStrFld(1);
                                String secondFld = t.getStrFld(5);
                                String concatFld = firstFld+secondFld;
                                tp.setHdr((short)3, newJtype, Jsizes);
                                tp.setStrFld(1, concatFld);
                                tp.setStrFld(2, firstFld);
                                tp.setStrFld(3, secondFld);
                                intermediateFile.insertNode(tp.getTupleByteArray());
                        }
                } catch (Exception e) {
                        e.printStackTrace();
                }
        try {
                fam = new FileScan(PQ_INTERMEDIATE_FILENAME,newJtype,
                        Jsizes, (short) newJtype.length, (short)newJtype.length, JProjList, null);
                        Tuple t =null;
                        int[] newfldsArray = {2,3};
                        DuplElim dup = new DuplElim(newJtype, (short)newJtype.length, Jsizes, fam, 10, false);
                        while ((t = dup.get_next()) != null){
                                t.print(newJtype, newfldsArray);
                                //t.print(Jtypes);
                        }
                        fam.close();
                        intermediateFile.deleteFile();
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
}