package heap;

import global.NID;
import global.PageId;
import global.RID;

import java.io.IOException;

import diskmgr.Page;

public class NodeHeapFile extends Heapfile{
	
	public NodeHeapFile(String name) throws HFException, HFBufMgrException,
			HFDiskMgrException, IOException {
		super(name);
		// TODO Auto-generated constructor stub
	}

	public void deleteFile() throws HFDiskMgrException, FileAlreadyDeletedException, HFBufMgrException, InvalidSlotNumberException, InvalidTupleSizeException, IOException{
		//todo implement delete
		   if(_file_deleted ) 
			   	throw new FileAlreadyDeletedException(null, "file alread deleted");
			      
			      
			      // Mark the deleted flag (even if it doesn't get all the way done).
			      _file_deleted = true;
			      
			      // Deallocate all data pages
			      PageId currentDirPageId = new PageId();
			      currentDirPageId.pid = _firstDirPageId.pid;
			      PageId nextDirPageId = new PageId();
			      nextDirPageId.pid = 0;
			      Page pageinbuffer = new Page();
			      NHFpage currentDirPage =  new NHFpage();
			      Node atuple;
			      
			      pinPage(currentDirPageId, currentDirPage, false);
			      //currentDirPage.openHFpage(pageinbuffer);
			      
			      NID nid = new NID();
			      while(currentDirPageId.pid != INVALID_PAGE)
				{      
				  for(nid = currentDirPage.firstRecord();
				      nid != null;
				      nid = currentDirPage.nextRecord(nid))
				    {
				      atuple = currentDirPage.getRecord(nid);
				      DataPageInfo dpinfo = new DataPageInfo( atuple);
				      //int dpinfoLen = arecord.length;
				      
				      freePage(dpinfo.pageId);
				      
				    }
				  // ASSERTIONS:
				  // - we have freePage()'d all data pages referenced by
				  // the current directory page.
				  
				  nextDirPageId = currentDirPage.getNextPage();
				  freePage(currentDirPageId);
				  
				  currentDirPageId.pid = nextDirPageId.pid;
				  if (nextDirPageId.pid != INVALID_PAGE) 
				    {
				      pinPage(currentDirPageId, currentDirPage, false);
				      //currentDirPage.openHFpage(pageinbuffer);
				    }
				}
			      
			      delete_file_entry( _fileName );
	}
	
	public boolean deleteNode(NID nid) throws InvalidTupleSizeException, HFException, HFBufMgrException, HFDiskMgrException, Exception
	{
		boolean status;
	      NHFpage currentDirPage = new NHFpage();
	      PageId currentDirPageId = new PageId();
	      NHFpage currentDataPage = new NHFpage();
	      PageId currentDataPageId = new PageId();
	      NID currentDataPageNid = new NID();
	      
	      status = _findDataPage(nid,
				     currentDirPageId, currentDirPage, 
				     currentDataPageId, currentDataPage,
				     currentDataPageNid);
	      
	      if(status != true) return status;	// record not found
	      
	      // ASSERTIONS:
	      // - currentDirPage, currentDirPageId valid and pinned
	      // - currentDataPage, currentDataPageid valid and pinned
	      
	      // get datapageinfo from the current directory page:
	      Node atuple;	
	      
	      atuple = currentDirPage.returnRecord(currentDataPageNid);
	      DataPageInfo pdpinfo = new DataPageInfo(atuple);
	      
	      // delete the record on the datapage
	      currentDataPage.deleteRecord(nid);
	      
	      pdpinfo.recct--;
	      pdpinfo.flushToTuple();	//Write to the buffer pool
	      if (pdpinfo.recct >= 1) 
		{
		  // more records remain on datapage so it still hangs around.  
		  // we just need to modify its directory entry
		  
		  pdpinfo.availspace = currentDataPage.available_space();
		  pdpinfo.flushToTuple();
		  unpinPage(currentDataPageId, true /* = DIRTY*/);
		  
		  unpinPage(currentDirPageId, true /* = DIRTY */);
		  
		  
		}
	      else
		{
		  // the record is already deleted:
		  // we're removing the last record on datapage so free datapage
		  // also, free the directory page if 
		  //   a) it's not the first directory page, and 
		  //   b) we've removed the last DataPageInfo record on it.
		  
		  // delete empty datapage: (does it get unpinned automatically? -NO, Ranjani)
		  unpinPage(currentDataPageId, false /*undirty*/);
		  
		  freePage(currentDataPageId);
		  
		  // delete corresponding DataPageInfo-entry on the directory page:
		  // currentDataPageRid points to datapage (from for loop above)
		  
		  currentDirPage.deleteRecord(currentDataPageNid);
		  
		  
		  // ASSERTIONS:
		  // - currentDataPage, currentDataPageId invalid
		  // - empty datapage unpinned and deleted
		  
		  // now check whether the directory page is empty:
		  
		  currentDataPageNid = currentDirPage.firstRecord();
		  
		  // st == OK: we still found a datapageinfo record on this directory page
		  PageId pageId;
		  pageId = currentDirPage.getPrevPage();
		  if((currentDataPageNid == null)&&(pageId.pid != INVALID_PAGE))
		    {
		      // the directory-page is not the first directory page and it is empty:
		      // delete it
		      
		      // point previous page around deleted page:
		      
		      HFPage prevDirPage = new HFPage();
		      pinPage(pageId, prevDirPage, false);

		      pageId = currentDirPage.getNextPage();
		      prevDirPage.setNextPage(pageId);
		      pageId = currentDirPage.getPrevPage();
		      unpinPage(pageId, true /* = DIRTY */);
		      
		      
		      // set prevPage-pointer of next Page
		      pageId = currentDirPage.getNextPage();
		      if(pageId.pid != INVALID_PAGE)
			{
			  HFPage nextDirPage = new HFPage();
			  pageId = currentDirPage.getNextPage();
			  pinPage(pageId, nextDirPage, false);
			  
			  //nextDirPage.openHFpage(apage);
			  
			  pageId = currentDirPage.getPrevPage();
			  nextDirPage.setPrevPage(pageId);
			  pageId = currentDirPage.getNextPage();
			  unpinPage(pageId, true /* = DIRTY */);
			  
			}
		      
		      // delete empty directory page: (automatically unpinned?)
		      unpinPage(currentDirPageId, false/*undirty*/);
		      freePage(currentDirPageId);
		      
		      
		    }
		  else
		    {
		      // either (the directory page has at least one more datapagerecord
		      // entry) or (it is the first directory page):
		      // in both cases we do not delete it, but we have to unpin it:
		      
		      unpinPage(currentDirPageId, true /* == DIRTY */);
		      
		      
		    }
		}
	      return true;
	}
	public int getNodeCnt() throws HFBufMgrException, InvalidSlotNumberException, InvalidTupleSizeException, IOException
	{
		 int answer = 0;
	      PageId currentDirPageId = new PageId(_firstDirPageId.pid);
	      
	      PageId nextDirPageId = new PageId(0);
	      
	      NHFpage currentDirPage = new NHFpage();
	      Page pageinbuffer = new Page();
	      
	      while(currentDirPageId.pid != INVALID_PAGE)
		{
		   pinPage(currentDirPageId, currentDirPage, false);
		   
		   NID nid = new NID();
		   Node atuple;
		   for (nid = currentDirPage.firstRecord();
		        nid != null;	// rid==NULL means no more record
		        nid = currentDirPage.nextRecord(nid))
		     {
		       atuple = currentDirPage.getRecord(nid);
		       DataPageInfo dpinfo = new DataPageInfo(atuple);
		       
		       answer += dpinfo.recct;
		     }
		   
		   // ASSERTIONS: no more record
	           // - we have read all datapage records on
	           //   the current directory page.
		   
		   nextDirPageId = currentDirPage.getNextPage();
		   unpinPage(currentDirPageId, false /*undirty*/);
		   currentDirPageId.pid = nextDirPageId.pid;
		}
	      
	      // ASSERTIONS:
	      // - if error, exceptions
	      // - if end of heapfile reached: currentDirPageId == INVALID_PAGE
	      // - if not yet end of heapfile: currentDirPageId valid
	      
	      
	      return answer; 
	}
    public Node getNode(NID nid) throws InvalidTupleSizeException, HFException, HFBufMgrException, HFDiskMgrException, Exception
    {
    	boolean status;
        NHFpage dirPage = new NHFpage();
        PageId currentDirPageId = new PageId();
        NHFpage dataPage = new NHFpage();
        PageId currentDataPageId = new PageId();
        NID currentDataPageNid = new NID();
        
        status = _findDataPage(nid,
  			     currentDirPageId, dirPage, 
  			     currentDataPageId, dataPage,
  			     currentDataPageNid);
        
        if(status != true) return null; // record not found 
        
        Node atuple = new Node();
        atuple = dataPage.getRecord(nid);
        
        /*
         * getRecord has copied the contents of rid into recPtr and fixed up
         * recLen also.  We simply have to unpin dirpage and datapage which
         * were originally pinned by _findDataPage.
         */    
        
        unpinPage(currentDataPageId,false /*undirty*/);
        
        unpinPage(currentDirPageId,false /*undirty*/);
        
        
        return  atuple;  //(true?)OK, but the caller need check if atuple==NULL
        
    }
   public NID insertNode(byte[] nodePtr) throws IOException, HFBufMgrException, InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException, SpaceNotAvailableException
    {
	   int dpinfoLen = 0;	
	      int recLen = nodePtr.length;
	      boolean found;
	      NID currentDataPageNid = new NID();
	      Page pageinbuffer = new Page();
	      NHFpage currentDirPage = new NHFpage();
	      NHFpage currentDataPage = new NHFpage();
	      
	      NHFpage nextDirPage = new NHFpage(); 
	      PageId currentDirPageId = new PageId(_firstDirPageId.pid);
	      PageId nextDirPageId = new PageId();  // OK
	      
	      pinPage(currentDirPageId, currentDirPage, false/*Rdisk*/);
	      
	      found = false;
	      Node atuple;
	      DataPageInfo dpinfo = new DataPageInfo();
	      while (found == false)
		{ //Start While01
		  // look for suitable dpinfo-struct
		  for (currentDataPageNid = currentDirPage.firstRecord();
		       currentDataPageNid != null;
		       currentDataPageNid = 
			 currentDirPage.nextRecord(currentDataPageNid))
		    {
		      atuple = currentDirPage.getRecord(currentDataPageNid);
		      
		      dpinfo = new DataPageInfo(atuple);
		      
		      // need check the record length == DataPageInfo'slength
		      
		       if(recLen <= dpinfo.availspace)
			 {
			   found = true;
			   break;
			 }  
		    }
		  
		  // two cases:
		  // (1) found == true:
		  //     currentDirPage has a datapagerecord which can accomodate
		  //     the record which we have to insert
		  // (2) found == false:
		  //     there is no datapagerecord on the current directory page
		  //     whose corresponding datapage has enough space free
		  //     several subcases: see below
		  if(found == false)
		    { //Start IF01
		      // case (2)
		      
		      //System.out.println("no datapagerecord on the current directory is OK");
		      //System.out.println("dirpage availspace "+currentDirPage.available_space());
		      
		      // on the current directory page is no datapagerecord which has
		      // enough free space
		      //
		      // two cases:
		      //
		      // - (2.1) (currentDirPage->available_space() >= sizeof(DataPageInfo):
		      //         if there is enough space on the current directory page
		      //         to accomodate a new datapagerecord (type DataPageInfo),
		      //         then insert a new DataPageInfo on the current directory
		      //         page
		      // - (2.2) (currentDirPage->available_space() <= sizeof(DataPageInfo):
		      //         look at the next directory page, if necessary, create it.
		      
		      if(currentDirPage.available_space() >= dpinfo.size)
			{ 
			  //Start IF02
			  // case (2.1) : add a new data page record into the
			  //              current directory page
			  currentDataPage = (NHFpage) _newDatapage(dpinfo); 
			  // currentDataPage is pinned! and dpinfo->pageId is also locked
			  // in the exclusive mode  
			  
			  // didn't check if currentDataPage==NULL, auto exception
			  
			  
			  // currentDataPage is pinned: insert its record
			  // calling a HFPage function
			  
			  
			  
			  atuple = (Node) dpinfo.convertToTuple();
			  
			  byte [] tmpData = atuple.getTupleByteArray();
			  currentDataPageNid = currentDirPage.insertRecord(tmpData);
			  
			  RID tmprid = currentDirPage.firstRecord();
			  
			  
			  // need catch error here!
			  if(currentDataPageNid == null)
			    throw new HFException(null, "no space to insert rec.");  
			  
			  // end the loop, because a new datapage with its record
			  // in the current directorypage was created and inserted into
			  // the heapfile; the new datapage has enough space for the
			  // record which the user wants to insert
			  
			  found = true;
			  
			} //end of IF02
		      else
			{  //Start else 02
			  // case (2.2)
			  nextDirPageId = currentDirPage.getNextPage();
			  // two sub-cases:
			  //
			  // (2.2.1) nextDirPageId != INVALID_PAGE:
			  //         get the next directory page from the buffer manager
			  //         and do another look
			  // (2.2.2) nextDirPageId == INVALID_PAGE:
			  //         append a new directory page at the end of the current
			  //         page and then do another loop
			    
			  if (nextDirPageId.pid != INVALID_PAGE) 
			    { //Start IF03
			      // case (2.2.1): there is another directory page:
			      unpinPage(currentDirPageId, false);
			      
			      currentDirPageId.pid = nextDirPageId.pid;
			      
			      pinPage(currentDirPageId,
							    currentDirPage, false);
			      
			      
			      
			      // now go back to the beginning of the outer while-loop and
			      // search on the current directory page for a suitable datapage
			    } //End of IF03
			  else
			    {  //Start Else03
			      // case (2.2): append a new directory page after currentDirPage
			      //             since it is the last directory page
			      nextDirPageId = newPage(pageinbuffer, 1);
			      // need check error!
			      if(nextDirPageId == null)
				throw new HFException(null, "can't new pae");
			      
			      // initialize new directory page
			      nextDirPage.init(nextDirPageId, pageinbuffer);
			      PageId temppid = new PageId(INVALID_PAGE);
			      nextDirPage.setNextPage(temppid);
			      nextDirPage.setPrevPage(currentDirPageId);
			      
			      // update current directory page and unpin it
			      // currentDirPage is already locked in the Exclusive mode
			      currentDirPage.setNextPage(nextDirPageId);
			      unpinPage(currentDirPageId, true/*dirty*/);
			      
			      currentDirPageId.pid = nextDirPageId.pid;
			      currentDirPage = new NHFpage(nextDirPage);
			      
			      // remark that MINIBASE_BM->newPage already
			      // pinned the new directory page!
			      // Now back to the beginning of the while-loop, using the
			      // newly created directory page.
			      
			    } //End of else03
			} // End of else02
		      // ASSERTIONS:
		      // - if found == true: search will end and see assertions below
		      // - if found == false: currentDirPage, currentDirPageId
		      //   valid and pinned
		      
		    }//end IF01
		  else
		    { //Start else01
		      // found == true:
		      // we have found a datapage with enough space,
		      // but we have not yet pinned the datapage:
		      
		      // ASSERTIONS:
		      // - dpinfo valid
		      
		      // System.out.println("find the dirpagerecord on current page");
		      
		      pinPage(dpinfo.pageId, currentDataPage, false);
		      //currentDataPage.openHFpage(pageinbuffer);
		      
		      
		    }//End else01
		} //end of While01
	      
	      // ASSERTIONS:
	      // - currentDirPageId, currentDirPage valid and pinned
	      // - dpinfo.pageId, currentDataPageRid valid
	      // - currentDataPage is pinned!
	      
	      if ((dpinfo.pageId).pid == INVALID_PAGE) // check error!
		throw new HFException(null, "invalid PageId");
	      
	      if (!(currentDataPage.available_space() >= recLen))
		throw new SpaceNotAvailableException(null, "no available space");
	      
	      if (currentDataPage == null)
		throw new HFException(null, "can't find Data page");
	      
	      
	      NID nid;
	      nid = currentDataPage.insertRecord(nodePtr);
	      
	      dpinfo.recct++;
	      dpinfo.availspace = currentDataPage.available_space();
	      
	      
	      unpinPage(dpinfo.pageId, true /* = DIRTY */);
	      
	      // DataPage is now released
	      atuple = currentDirPage.returnRecord(currentDataPageNid);
	      DataPageInfo dpinfo_ondirpage = new DataPageInfo(atuple);
	      
	      
	      dpinfo_ondirpage.availspace = dpinfo.availspace;
	      dpinfo_ondirpage.recct = dpinfo.recct;
	      dpinfo_ondirpage.pageId.pid = dpinfo.pageId.pid;
	      dpinfo_ondirpage.flushToTuple();
	      
	      
	      unpinPage(currentDirPageId, true /* = DIRTY */);
	      
	      
	      return nid;
    }
   public Scan openScan() throws InvalidTupleSizeException, IOException
   {
	   Scan newscan = new Scan(this);
	      return newscan;  
   }
   public boolean updateNode(NID nid, Node newNode) throws InvalidTupleSizeException, HFException, HFBufMgrException, HFDiskMgrException, Exception
   {
	   boolean status;
	      NHFpage dirPage = new NHFpage();
	      PageId currentDirPageId = new PageId();
	      NHFpage dataPage = new NHFpage();
	      PageId currentDataPageId = new PageId();
	      NID currentDataPageNid = new NID();
	      
	      status = _findDataPage(nid,
				     currentDirPageId, dirPage, 
				     currentDataPageId, dataPage,
				     currentDataPageNid);
	      
	      if(status != true) return status;	// record not found
	      Node atuple = new Node();
	      atuple = dataPage.returnRecord(nid);
	      
	      // Assume update a record with a record whose length is equal to
	      // the original record
	      
	      if(newNode.getLength() != atuple.getLength())
		{
		  unpinPage(currentDataPageId, false /*undirty*/);
		  unpinPage(currentDirPageId, false /*undirty*/);
		  
		  throw new InvalidUpdateException(null, "invalid record update");
		  
		}

	      // new copy of this record fits in old space;
	      atuple.tupleCopy(newNode);
	      unpinPage(currentDataPageId, true /* = DIRTY */);
	      
	      unpinPage(currentDirPageId, false /*undirty*/);
	      
	      
	      return true;
   }
   
}
