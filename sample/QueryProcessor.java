//$Id$
package sample;

import java.util.List;

public class QueryProcessor {

	public static void main(String[] args) throws Exception {
		//TODO: new input data diff in some format
		//TODO: get the subscriber conditions, parse it and match with the input data
		//Parsing query conditions format : "subcriber" "col_name=value&col_name1=value1|"
		String query = "item=12345_OR_title!=DS_OR_author=krish";
		List<Criteria> criterias = Criteria.parseQueryString(query);
		for(Criteria c : criterias) {
			System.out.println(c);
		}
		
		
	}
}
