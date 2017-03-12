//package tests;

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
  
   public static void main (String[] args) throws FileNotFoundException
   {
    BatchNodeDeleteHandler BN = new BatchNodeDeleteHandler();
	if(args.length==2){
	 try{
	  BN.runbatchnodedelete(args[0],args[1]);
	  }
	 catch(Exception e){
	  System.out.println (""+e);	
	  }
	}
	else{
	 System.out.println("Improper Arguments");
        }
   } 

 
}
