/*Author Harshdeep*/
package tests;

import java.io.*;
import java.util.*;
import java.lang.*;
import heap.*;
import bufmgr.*;
import diskmgr.*;
import global.*;
import btree.*;


class BatchNodeDelete implements GlobalConst
{	
	public void runDeleteNode(String args[]) {
		BatchNodeDeleteHandler BN = SystemDefs.JavabaseDB.getBatchNodeDeleteHandler();

			try{
				BN.runbatchnodedelete(args[2],args[1]);
			}
			catch(Exception e){
				System.out.println (""+e);	
			}
		
	}
}
