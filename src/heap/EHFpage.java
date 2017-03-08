package heap;

import global.Convert;
import global.EID;
import global.NID;
import global.PageId;

import java.io.IOException;

public class EHFpage extends HFPage{
	 public EHFpage(EHFpage nextDirPage) {
		 data = nextDirPage.getpage();
		// TODO Auto-generated constructor stub
	}
	public EHFpage() {
		// TODO Auto-generated constructor stub
	}
	public EID insertRecord ( byte [] record)		
			    throws IOException
			    {
			      EID eid = new EID();
			      
			      int recLen = record.length;
			      int spaceNeeded = recLen + SIZE_OF_SLOT;
			      
			      // Start by checking if sufficient space exists.
			      // This is an upper bound check. May not actually need a slot
			      // if we can find an empty one.
			      
			      freeSpace = Convert.getShortValue (FREE_SPACE, data);
			      if (spaceNeeded > freeSpace) {
			        return null;
				
			      } else {
				
				// look for an empty slot
				slotCnt = Convert.getShortValue (SLOT_CNT, data); 
				int i; 
				short length;
				for (i= 0; i < slotCnt; i++) 
				  {
				    length = getSlotLength(i); 
				    if (length == EMPTY_SLOT)
				      break;
				  }
				
				if(i == slotCnt)   //use a new slot
				  {           
				    // adjust free space        
				    freeSpace -= spaceNeeded;
				    Convert.setShortValue (freeSpace, FREE_SPACE, data);
				    
				    slotCnt++;
				    Convert.setShortValue (slotCnt, SLOT_CNT, data);
				    
				  }
				else {
				  // reusing an existing slot
				  freeSpace -= recLen;
				  Convert.setShortValue (freeSpace, FREE_SPACE, data);
				}
			        
				usedPtr = Convert.getShortValue (USED_PTR, data);
			        usedPtr -= recLen;    // adjust usedPtr
				Convert.setShortValue (usedPtr, USED_PTR, data);
				
				//insert the slot info onto the data page
				setSlot(i, recLen, usedPtr);   
				
				// insert data onto the data page
				System.arraycopy (record, 0, data, usedPtr, recLen);
				curPage.pid = Convert.getIntValue (CUR_PAGE, data);
				eid.pageNo.pid = curPage.pid;
				eid.slotNo = i;
				return   eid ;
			      }
			    } 
	public void deleteRecord ( EID eid )
		    throws IOException,  
			   InvalidSlotNumberException
		    {
		      int slotNo = eid.slotNo;
		      short recLen = getSlotLength (slotNo);
		      slotCnt = Convert.getShortValue (SLOT_CNT, data);
		      
		      // first check if the record being deleted is actually valid
		      if ((slotNo >= 0) && (slotNo < slotCnt) && (recLen > 0))
			{ 
			  // The records always need to be compacted, as they are
			  // not necessarily stored on the page in the order that
			  // they are listed in the slot index.
			  
			  // offset of record being deleted
			  int offset = getSlotOffset(slotNo); 
			  usedPtr = Convert.getShortValue (USED_PTR, data);
			  int newSpot= usedPtr + recLen;
			  int size = offset - usedPtr;
			  
			  // shift bytes to the right
			  System.arraycopy(data, usedPtr, data, newSpot, size);
			  
			  // now need to adjust offsets of all valid slots that refer
			  // to the left of the record being removed. (by the size of the hole)
			  
			  int i, n, chkoffset;
			  for (i = 0, n = DPFIXED; i < slotCnt; n +=SIZE_OF_SLOT, i++) {
			    if ((getSlotLength(i) >= 0))
			      {
				chkoffset = getSlotOffset(i);
				if(chkoffset < offset)
				  {
				    chkoffset += recLen;
				    Convert.setShortValue((short)chkoffset, n+2, data);
				  }
			      }
			  }
			  
			  // move used Ptr forwar
			  usedPtr += recLen;   
			  Convert.setShortValue (usedPtr, USED_PTR, data);
			  
			  // increase freespace by size of hole
			  freeSpace = Convert.getShortValue(FREE_SPACE, data);
			  freeSpace += recLen;  
			  Convert.setShortValue (freeSpace, FREE_SPACE, data);
			  
			  setSlot(slotNo, EMPTY_SLOT, 0);  // mark slot free
			} 
		      else {
			throw new InvalidSlotNumberException (null, "HEAPFILE: INVALID_SLOTNO");
		      }
		    }
	  public EID firstRecord() 
			    throws IOException
			    {
			      EID eid = new EID();
			      // find the first non-empty slot
			      
			      
			      slotCnt = Convert.getShortValue (SLOT_CNT, data);
			      
			      int i;
			      short length;
			      for (i= 0; i < slotCnt; i++)
				{
				  length = getSlotLength (i);
				  if (length != EMPTY_SLOT)
				    break;
				}
			      
			      if(i== slotCnt)
				return null;
			      
			      // found a non-empty slot
			      
			      eid.slotNo = i;
			      curPage.pid= Convert.getIntValue(CUR_PAGE, data);
			      eid.pageNo.pid = curPage.pid;
			      
			      return eid;
			    }
	  public EID nextRecord (EID curNid) 
			    throws IOException 
			    {
			      EID eid = new EID();
			      slotCnt = Convert.getShortValue (SLOT_CNT, data);
			      
			      int i=curNid.slotNo;
			      short length; 
			      
			      // find the next non-empty slot
			      for (i++; i < slotCnt;  i++)
				{
				  length = getSlotLength(i);
				  if (length != EMPTY_SLOT)
				    break;
				}
			      
			      if(i >= slotCnt)
				return null;
			      
			      // found a non-empty slot
			      
			      eid.slotNo = i;
			      curPage.pid = Convert.getIntValue(CUR_PAGE, data);
			      eid.pageNo.pid = curPage.pid;
			      
			      return eid;
			    }
	  public Edge getRecord ( EID eid ) 
			    throws IOException,  
				   InvalidSlotNumberException
			    {
			      short recLen;
			      short offset;
			      byte []record;
			      PageId pageNo = new PageId();
			      pageNo.pid= eid.pageNo.pid;
			      curPage.pid = Convert.getIntValue (CUR_PAGE, data);
			      int slotNo = eid.slotNo;
			      
			      // length of record being returned
			      recLen = getSlotLength (slotNo);
			      slotCnt = Convert.getShortValue (SLOT_CNT, data);
			      if (( slotNo >=0) && (slotNo < slotCnt) && (recLen >0) 
				  && (pageNo.pid == curPage.pid))
				{
				  offset = getSlotOffset (slotNo);
				  record = new byte[recLen];
				  System.arraycopy(data, offset, record, 0, recLen);
				  Edge tuple = new Edge();
				  return tuple;
				}
			      
			      else {
			        throw new InvalidSlotNumberException (null, "HEAPFILE: INVALID_SLOTNO");
			      }
			     
			      
			    }
	  public Node returnRecord ( EID eid )
			    throws IOException, 
				   InvalidSlotNumberException
			    {
			      short recLen;
			      short offset;
			      PageId pageNo = new PageId();
			      pageNo.pid = eid.pageNo.pid;
			      
			      curPage.pid = Convert.getIntValue (CUR_PAGE, data);
			      int slotNo = eid.slotNo;
			      
			      // length of record being returned
			      recLen = getSlotLength (slotNo);
			      slotCnt = Convert.getShortValue (SLOT_CNT, data);
			      
			      if (( slotNo >=0) && (slotNo < slotCnt) && (recLen >0)
				  && (pageNo.pid == curPage.pid))
				{
				  
				  offset = getSlotOffset (slotNo);
				  Node tuple = new Node();
				  return tuple;
				}
			      
			      else {   
			        throw new InvalidSlotNumberException (null, "HEAPFILE: INVALID_SLOTNO");
			      }
			      
			    }
}
