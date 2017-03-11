package heap;

import global.AttrType;
import global.Convert;
import global.EID;
import global.NID;

import java.io.IOException;



public class Edge extends Tuple{
	private String label;
	private NID source;
	private NID destination;
	private int weight;
	public Edge()
	{
		super();
		
	}
    public Edge(byte[] anode, int offset)
    {
    	super(anode, offset, anode.length);
    }
    
    public Edge(byte[] anode, int offset, int length)
    {
    	super(anode, offset, length);
    }
    
	public Edge(Edge fromEdge)
	{
		super(fromEdge);
		this.source=fromEdge.source;
		this.label=fromEdge.label;
		this.destination=fromEdge.destination;
		this.weight=fromEdge.weight;
	}
	public String getLabel()
	{
		return label;
	}
	public int getWeight()
	{
		return weight;
	}
	public NID getSource()
	{
		return source;
	
	}
	public NID getDestination()
	{
		return destination;
	}
	public Edge setLabel(String Label)
	{
		label=Label;
		return null;
	}
	public Edge setWeight(int Weight)
	{
		weight=Weight;
		return null;
	}
	public Edge setSource(NID sourceID)
	{
		source=sourceID;
		return null;
	}
	public Edge setDestination(NID destID )
	{
		destination=destID;
		return null;
	}
	public byte[] getEdgeByteArray()
	{
		 return getTupleByteArray();
	}
	public void print(AttrType type[]) throws IOException
	{
		 int i, val;
		  float fval;
		  String sval;

		  System.out.print("[");
		  for (i=0; i< fldCnt-1; i++)
		   {
		    switch(type[i].attrType) {

		   case AttrType.attrInteger:
		     val = Convert.getIntValue(fldOffset[i], data);
		     System.out.print(val);
		     break;

		   case AttrType.attrReal:
		     fval = Convert.getFloValue(fldOffset[i], data);
		     System.out.print(fval);
		     break;

		   case AttrType.attrString:
		     sval = Convert.getStrValue(fldOffset[i], data,fldOffset[i+1] - fldOffset[i]);
		     System.out.print(sval);
		     break;
		  
		   case AttrType.attrNull:
		   case AttrType.attrSymbol:
		     break;
		   }
		   System.out.print(", ");
		 } 
		 
		 switch(type[fldCnt-1].attrType) {

		   case AttrType.attrInteger:
		     val = Convert.getIntValue(fldOffset[i], data);
		     System.out.print(val);
		     break;

		   case AttrType.attrReal:
		     fval = Convert.getFloValue(fldOffset[i], data);
		     System.out.print(fval);
		     break;

		   case AttrType.attrString:
		     sval = Convert.getStrValue(fldOffset[i], data,fldOffset[i+1] - fldOffset[i]);
		     System.out.print(sval);
		     break;

		   case AttrType.attrNull:
		   case AttrType.attrSymbol:
		     break;
		   }
		   System.out.println("]");
	}
	public void edgeCopy(Node fromNode)
	{
		 byte [] temparray = fromNode.getNodeByteArray();
	     System.arraycopy(temparray, 0, data, tuple_offset, tuple_length);  
		
	}
	public void edgeInit(byte[] aEdge, int offset)
	{
		 tupleInit(aEdge, offset, aEdge.length);
	
	}
	public void edgeSet(byte[] fromEdge, int offset)
	{
		tupleSet(fromEdge, offset, fromEdge.length);
	     
	}
}
