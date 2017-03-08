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
		 data = new byte[max_size];
	       tuple_offset = 0;
	       tuple_length = max_size;
	}
	public void Edge(byte[] aedge, int offset)
	{
		 data = aedge;
	      tuple_offset = offset;
	      
	}
	public void Edge(Edge fromEdge)
	{
		data = fromEdge.getTupleByteArray();
	       tuple_length = fromEdge.getLength();
	       tuple_offset = 0;
	       fldCnt = fromEdge.noOfFlds(); 
	       fldOffset = fromEdge.copyFldOffset(); 
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
	byte[] getNodeByteArray()
	{
		byte [] tuplecopy = new byte [tuple_length];
	       System.arraycopy(data, tuple_offset, tuplecopy, 0, tuple_length);
	       return tuplecopy;
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
	public short size()
	{
		return ((short) (fldOffset[fldCnt] - tuple_offset));
	}
	public void edgeCopy(Edge fromEdge)
	{
		byte [] temparray = fromEdge.getTupleByteArray();
	       System.arraycopy(temparray, 0, data, tuple_offset, tuple_length);
	}
	public void edgeInit(byte[] aedge, int offset)
	{
		data = aedge;
	      tuple_offset = offset;
	      
	}
	public void edgeSet(byte[] fromedge, int offset)
	{
		System.arraycopy(fromedge, offset, data, 0, offset);
	      tuple_offset = 0;
	     
	}
}
