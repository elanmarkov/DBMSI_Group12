package global;

/** 
 * Enumeration class for AttrType
 * 
 */

public class ExpType {

  public static final int expNodeLabel  = 0;
  public static final int expEdgeLabel  = 1;
  public static final int expWeight  = 2;
  public static final int expNoOfEdges  = 3;
  public static final int expDesc    = 4;
  public static final int expTotalWeights    = 5;
  
  public int expType;

  public ExpType (int _expType) {
	  expType = _expType;
  }

  public String toString() {

    switch (expType) {
    case expNodeLabel:
      return "expNodeLabel";
    case expEdgeLabel:
        return "expEdgeLabel";
    case expWeight:
    	return "expWeight";
    case expNoOfEdges:
    	return "expNoOfEdges";
    case expDesc:
      return "expDesc";
    case expTotalWeights:
    	return "expTotalWeights";
    }
    return ("Unexpected ExpType " + expType);
  }
}
