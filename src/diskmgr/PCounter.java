/*
Page Counter by Elan Markov, Group 12
Counts number of read_page/write_page operations executed.
Use initialize() to start a new count.
All variables and methods are static. 

Copied from specification document code snippet.
*/
package diskmgr;
public class PCounter {
	public static int rcounter;
	public static int wcounter;
	public static void initialize() {
		rcounter = 0;
		wcounter = 0;
	}
	public static void readIncrement() {
		rcounter++;
	}
	public static void writeIncrement() {
		wcounter++;
	}
}
