//$Id$
package sample;

public class QueryTree {

	private QueryTree leftQuery = null;
	private QueryTree rightQuery = null;
	
	public boolean hasLeftQuery() {
		return this.leftQuery != null;
	}
	
	public boolean hasRightQuery() {
		return this.rightQuery != null;
	}
	
	
}
