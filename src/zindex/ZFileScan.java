package zindex;

/*
 * @(#) BTIndexPage.java   98/05/14
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *         Author: Xiaohu Li (xioahu@cs.wisc.edu)
 *
 */
import java.io.IOException;
import java.util.Stack;

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
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import catalog.Utility;
import global.Catalogglobal;
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
  private static final int NUMBER_OF_MISSES = 32;
  private BTreeFile bFile;
  private KeyClass loKey;
  private KeyClass hiKey;
  private int distance;
  private KeyClass center;
  private BTFileScan bTreeScan;
  private Stack<StringKey> stack;
  private StringKey lastReported;
  private StringKey low;
  private StringKey high;
  
  public ZFileScan(BTreeFile bFile, KeyClass loKey, KeyClass hiKey) throws KeyNotMatchException, IteratorException, ConstructPageException, PinPageException, UnpinPageException, IOException {
	this.bFile = bFile;
	this.loKey = loKey;
	this.center = null;
	this.hiKey = hiKey;
	this.distance = -1;
	this.stack = new Stack<StringKey>();
	
	if(loKey == null){
		Descriptor lDesc = new Descriptor();
		lDesc.set(0, 0, 0, 0, 0);
		this.loKey = new DescriptorKey(lDesc);
	}
	if(hiKey == null){
		Descriptor hDesc = new Descriptor();
		hDesc.set(10000, 10000, 10000, 10000, 10000);
		this.hiKey = new DescriptorKey(hDesc);
	}
	this.low = Utility.conver5Dto1D(this.loKey);
	this.high = Utility.conver5Dto1D(this.hiKey);
	this.lastReported = this.low;
	this.bTreeScan = bFile.new_scan(this.low, this.high);
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
	  this.bFile = bfile;
	  this.distance =distance;
	  this.stack = new Stack<StringKey>();
	  this.low = Utility.conver5Dto1D(this.loKey);
	  this.high = Utility.conver5Dto1D(this.hiKey);
	  this.lastReported = this.low;
	  this.bTreeScan = bFile.new_scan(this.low, this.high);
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
	  //KeyDataEntry entry = getNextDataInRangeOptimised();
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
			  if(!(centerPoint.distance(desc)<=this.distance)){
				  isInHyperRectangle = false;
			  }
		  }
	  }while(!isInHyperRectangle);
	return entry;
}


private KeyDataEntry getNextDataInRangeOptimised() throws ScanIteratorException {
	int misCounter =0;
	KeyDataEntry nextEntry =null;
	  boolean isInHyperRectangle =false;
	  Descriptor centerPoint = null;
	  if(this.center !=null){
		  centerPoint = ((DescriptorKey)this.center).getKey();
	  }
	  
	  do{
		  if(lastReported.getKey().compareTo(high.getKey())>0){
				if(!stack.isEmpty()){
					high = stack.pop();
					low = stack.pop();
					lastReported=low;
					destroyAndInitialiseANewScan();
					misCounter = 0;
				}else{
					break;
				}
			}
		  nextEntry = this.bTreeScan.get_next();
		  if(nextEntry == null){
			  return null;
		  }
		  lastReported = (StringKey) nextEntry.key;
		  String key = ((StringKey)nextEntry.key).getKey();
		  Descriptor desc = Utility.convert1Dto5D(key);
		  isInHyperRectangle = Utility.checkIfPointFallsWithInRange(
				  	((DescriptorKey)this.loKey).getKey(),
				  	((DescriptorKey)this.hiKey).getKey(),desc);
		  if(isInHyperRectangle){
			  misCounter=0;
			  if(centerPoint !=null){
				  // check if it point is inside the circle in case of distance search
				  if(!(centerPoint.distance(desc)<=this.distance)){
					  isInHyperRectangle = false;
				  }
			  }
		  }else{
			  misCounter++;
			  boolean isChanged = false;
			  if(misCounter>=NUMBER_OF_MISSES){
				  	StringKey bigMin = null;
				  	StringKey litMax = null;
				  	isChanged = true;
					do{
						ZDivide(low.getKey(),high.getKey());
						bigMin = stack.pop();
						litMax = stack.pop();
						if(lastReported.getKey().compareTo(litMax.getKey())<0){
							stack.push(bigMin);
							stack.push(this.high);
							this.high = litMax;
						}
						if(lastReported.getKey().compareTo(litMax.getKey())<0){
							this.high = litMax;
						}
					}while(lastReported.getKey().compareTo(litMax.getKey())>0);
					if(isChanged){
						misCounter = 0;
						this.low = bigMin;
						lastReported = low;
						destroyAndInitialiseANewScan();
						//System.out.println("basicFunctionTest.doRangeSearchOnZCurve() after zdivide low "+Integer.parseInt(low,2));
					}
			  }
		  }
	  }while(!isInHyperRectangle);
	return nextEntry;
}

private void destroyAndInitialiseANewScan() {
	try {
		this.bTreeScan.DestroyBTreeFileScan();
	} catch (InvalidFrameNumberException | ReplacerException | PageUnpinnedException
			| HashEntryNotFoundException | IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	try {
		this.bTreeScan = bFile.new_scan(this.low, this.high);
	} catch (KeyNotMatchException | IteratorException | ConstructPageException | PinPageException
			| UnpinPageException | IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
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

  private void ZDivide(String lowKey, String highKey) {
		int i=0;
		for(;i<lowKey.length();i++){
			if(lowKey.charAt(i) != highKey.charAt(i)){
				break;
			}
		}
		StringBuffer litMax = new StringBuffer(lowKey);
		StringBuffer bigMin = new StringBuffer(highKey);
		if(lowKey.charAt(i) == '1'){
			for(int temp = i+Catalogglobal.NUMBER_OF_DIMENSIONS;temp < lowKey.length();
					temp+=Catalogglobal.NUMBER_OF_DIMENSIONS){
				litMax.setCharAt(temp, '0');
				bigMin.setCharAt(temp, '1');
			}
		}else{
			for(int temp = i+Catalogglobal.NUMBER_OF_DIMENSIONS;temp < lowKey.length();
					temp+=Catalogglobal.NUMBER_OF_DIMENSIONS){
				litMax.setCharAt(temp, '1');
				bigMin.setCharAt(temp, '0');
			}
		}
		if(lowKey.charAt(i) == '1'){
			for(int temp = 0;temp < lowKey.length();temp++){
				if(temp%Catalogglobal.NUMBER_OF_DIMENSIONS != i%Catalogglobal.NUMBER_OF_DIMENSIONS){
					litMax.setCharAt(temp, lowKey.charAt(temp));
					bigMin.setCharAt(temp, highKey.charAt(temp));
				}
				
			}
		}else{
			for(int temp = 0;temp < lowKey.length();temp++){
				if(temp%Catalogglobal.NUMBER_OF_DIMENSIONS != i%Catalogglobal.NUMBER_OF_DIMENSIONS){
					litMax.setCharAt(temp, highKey.charAt(temp));
					bigMin.setCharAt(temp, lowKey.charAt(temp));
				}
			}
		}
		stack.push(new StringKey(litMax.toString()));
		stack.push(new StringKey(bigMin.toString()));
		//System.out.println("basicFunctionTest.ZDivide() low : "+litMax.toString());
		//System.out.println("basicFunctionTest.ZDivide() high : "+bigMin.toString());
	}
}






