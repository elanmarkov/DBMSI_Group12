package zindex;

/*
 * @(#) BTIndexPage.java   98/05/14
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *         Author: Xiaohu Li (xioahu@cs.wisc.edu)
 *
 */
import java.io.IOException;

import btree.BTFileScan;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.IndexFileScan;
import btree.IteratorException;
import btree.KeyClass;
import btree.KeyDataEntry;
import btree.KeyNotMatchException;
import btree.PinPageException;
import btree.ScanDeleteException;
import btree.ScanIteratorException;
import btree.StringKey;
import btree.UnpinPageException;
import catalog.Utility;
import global.Descriptor;
import global.GlobalConst;

/**
 * BTFileScan implements a search/iterate interface to B+ tree 
 * index files (class BTreeFile).  It derives from abstract base
 * class IndexFileScan.  
 */
public class ZFileScan  extends IndexFileScan
             implements  GlobalConst
{

  private BTreeFile bFile;
  private KeyClass loKey;
  private KeyClass hiKey;
  private int distance;
  private KeyClass center;
  private BTFileScan bTreeScan;
  
  public ZFileScan(BTreeFile bFile, KeyClass loKey, KeyClass hiKey) throws KeyNotMatchException, IteratorException, ConstructPageException, PinPageException, UnpinPageException, IOException {
	this.bFile = bFile;
	this.loKey = loKey;
	this.center = null;
	this.hiKey = hiKey;
	this.distance = -1;
	if(loKey == null){
		Descriptor lDesc = new Descriptor();
		lDesc.set(0, 0, 0, 0, 0);
		this.loKey = new DescriptorKey(lDesc);
	}
	if(hiKey == null){
		Descriptor lDesc = new Descriptor();
		lDesc.set(10000, 10000, 10000, 10000, 10000);
		this.loKey = new DescriptorKey(lDesc);
	}
		this.bTreeScan = bFile.new_scan(Utility.conver5Dto1D(this.loKey), Utility.conver5Dto1D(this.hiKey));
  }
  
  public ZFileScan(BTreeFile bfile, KeyClass key, int distance) throws KeyNotMatchException, IteratorException, ConstructPageException, PinPageException, UnpinPageException, IOException{
	  Descriptor low = new Descriptor();
	  Descriptor point =((DescriptorKey)key).getKey();
	  low.set(getCirclesLowerBound(distance, point.get(0)),
			  getCirclesLowerBound(distance, point.get(1)),
			  getCirclesLowerBound(distance, point.get(2)),
			  getCirclesLowerBound(distance, point.get(3)),
			  getCirclesLowerBound(distance, point.get(4)));
	  Descriptor high = new Descriptor();
	  high.set(getCirclesUpperBound(distance, point.get(0)),
			  getCirclesUpperBound(distance, point.get(1)),
			  getCirclesUpperBound(distance, point.get(2)),
			  getCirclesUpperBound(distance, point.get(3)),
			  getCirclesUpperBound(distance, point.get(4)));
	  this.loKey = new DescriptorKey(low);
	  this.hiKey = new DescriptorKey(high);
	  this.center = key;
	  this.bFile = bFile;
	  this.distance =distance;
	  this.bTreeScan = bFile.new_scan(Utility.conver5Dto1D(this.loKey), Utility.conver5Dto1D(this.hiKey));
  }

private int getCirclesLowerBound(int distance, int dimVal) {
	int val = dimVal-distance;
	  val = val>=0 ? val : 0;
	return val;
}

private int getCirclesUpperBound(int distance, int dimVal) {
	int val = dimVal+distance;
	  val = val<=10000 ? val : 10000;
	return val;
}

  public int getDistance() {
	return distance;
  }

public void setDistance(int distance) {
	this.distance = distance;
}


/**
   * Iterate once (during a scan).  
   *@return null if done; otherwise next KeyDataEntry
   *@exception ScanIteratorException iterator error
   */
  public KeyDataEntry get_next() 
    throws ScanIteratorException
    {
	  KeyDataEntry entry = getNextDataInRange();
	  return entry;
    }

private KeyDataEntry getNextDataInRange() throws ScanIteratorException {
	KeyDataEntry entry =null;
	  boolean isInHyperRectangle =false;
	  Descriptor centerPoint = null;
	  if(this.center !=null){
		  centerPoint = ((DescriptorKey)this.center).getKey();
	  }
	  do{
		  entry = this.bTreeScan.get_next();
		  if(entry == null){
			  return null;
		  }
		  String key = ((StringKey)entry.key).getKey();
		  Descriptor desc = Utility.convert1Dto5D(key);
		  isInHyperRectangle = Utility.checkIfPointFallsWithInRange(
				  	((DescriptorKey)this.loKey).getKey(),
				  	((DescriptorKey)this.hiKey).getKey(),desc);
		  if(isInHyperRectangle && centerPoint !=null){
			  // check if it point is inside the circle in case of distance search
			  if(this.distance > centerPoint.distance(desc)){
				  isInHyperRectangle = false;
			  }
		  }
	  }while(!isInHyperRectangle);
	return entry;
}


  /**
   * Delete currently-being-scanned(i.e., just scanned)
   * data entry.
   *@exception ScanDeleteException  delete error when scan
   */
  public void delete_current() 
    throws ScanDeleteException {
	  this.bTreeScan.delete_current();
    
  }
  
  /** max size of the key
   *@return the maxumum size of the key in BTFile
   */
  public int keysize() {
    return this.bTreeScan.keysize();
  }  
  
  
  
  /**
  * destructor.
  * unpin some pages if they are not unpinned already.
  * and do some clearing work.
  *@exception IOException  error from the lower layer
  *@exception bufmgr.InvalidFrameNumberException  error from the lower layer
  *@exception bufmgr.ReplacerException  error from the lower layer
  *@exception bufmgr.PageUnpinnedException  error from the lower layer
  *@exception bufmgr.HashEntryNotFoundException   error from the lower layer
  */
  public  void DestroyZCurveFileScan()
    throws  IOException, bufmgr.InvalidFrameNumberException,bufmgr.ReplacerException,
            bufmgr.PageUnpinnedException,bufmgr.HashEntryNotFoundException   
  { 
    this.bTreeScan.DestroyBTreeFileScan();
  }




}






