package iterator;
import heap.*;

public class FldSpec {
  public  RelSpec relation;
  public  int offset;
  public int offset2;
  
  /**contrctor
   *@param _relation the relation is outer or inner
   *@param _offset the offset of the field
   */
  public  FldSpec(RelSpec _relation, int _offset)
    {
      relation = _relation;
      offset = _offset;
      offset2 = -1;
    }
  public  FldSpec(RelSpec _relation, int _offset, int _offset2)
  {
    relation = _relation;
    offset = _offset;
    offset2 = _offset2;
  }
}

