//$Id$
package sample;

import java.util.ArrayList;
import java.util.List;

public class Criteria {

	private static final String AND = "_AND_";
	private static final String OR = "_OR_";
	
	private String attributeName = null;
	private String attributeValue = null;
	private Comparator comparator = null;
	
	public enum Comparator {

        EQUALS("="),
        NOT_EQUALS("!="),
        GREATER_THAN(">"),
        LESS_THAN("<"),
        GREATER_EQUALS(">="),
        LESS_EQUALS("<=");

        private final String operator;

        Comparator(String operator) {
            this.operator = operator;
        }
        
        public String getOperator() {
        	return this.operator;
        }
    }
	
	Criteria(String attributeName, String attributeValue, Comparator comparator) {
		this.attributeName = attributeName;
		this.attributeValue = attributeValue;
		this.comparator = comparator;
	}
	
	public static Criteria formCriteria(String criteria) {
    	for (Comparator comparator : Comparator.values()) {
            if (criteria.contains(comparator.getOperator())) {
            	String[] params = criteria.split(comparator.getOperator());
            	return new Criteria(params[0], params[1], comparator);
            }
        }
        return null;
    }
	
	public static List<Criteria> parseQueryString(String queryString) throws Exception {
		if(queryString == null) {
			return null;
		}
		if(queryString.contains(AND) && queryString.contains(OR)) {
			throw new Exception("Net Criterias not supported");
		}
		List<Criteria> criterias = new ArrayList<Criteria>();
		if(queryString.contains(AND)) {
			String[] params = queryString.split(AND);
			for(String param : params) {
				criterias.add(formCriteria(param));
			}
		} else {
			String[] params = queryString.split(OR);
			for(String param : params) {
				criterias.add(formCriteria(param));
			}
		}
		return criterias;
	}
	
	public static List<Criteria> parseQueryString1(String queryString) throws Exception {
		if(queryString == null) {
			return null;
		}
		int idx = 0;
		while(idx < queryString.length()) {
			idx = queryString.indexOf('\'');
			int endIdx = queryString.indexOf('\'', idx+1);
			String criteria = queryString.substring(idx, endIdx);
			Criteria c = formCriteria(criteria);
			
		}
		
		for(int i=0; i<queryString.length(); i++) {
			char eachChar = queryString.charAt(i);
			if(eachChar == '\'') {
				continue;
			}
		}
		return null;
	}
	
	@Override
	public String toString() {
		return this.attributeName + this.comparator.getOperator() + this.attributeValue;
	}
}
