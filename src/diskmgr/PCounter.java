/*
Page Counter by Elan Markov, Group 12
Counts number of read_page/write_page operations executed.
Use initialize() to start a new count.
All variables and methods are static. 

Copied from specification document code snippet.
*/
package diskmgr;
/** 
PCounter class.
Tracks reads and writes on databases.

All methods are static, so no values need to be passed - but only one counter can exist at a time.

Use initialize() to reset counts.
*/
public class PCounter {
	public static int rcounter;
	public static int wcounter;
	/** Initialize method. Resets counter values to 0. */
	public static void initialize() {
		rcounter = 0;
		wcounter = 0;
	}
	/** Increments the read count. */ 
	public static void readIncrement() {
		rcounter++;
	}
	/** Increments the write count. */
	public static void writeIncrement() {
		wcounter++;
	}
}
