package com.amazon.zama_edoc;

import java.util.ArrayList;
import java.util.Scanner;

import com.amazon.zama_edoc.cassandra.CassandraAdapter;

public class Prompt {
	
	private static String defaultLabel = "# ";
	private static String secondaryPromptLabel = "     > ";
	private static Scanner sc = null;
	private static boolean quitPrompt = true;
	private static boolean promptLabel = true;
	private static String subscriberName = null;
	private static String subscriberHostPort = null;
	private static ArrayList<String> queries = new ArrayList<String>();
	private static int itemId = 0;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// pre-load something..
		create();
	}

	private static void create() {
		// TODO Auto-generated method stub
		sc = new Scanner(System.in);
		while(quitPrompt) {
			printPromptLabel();
			process(sc.nextLine());
		}
	}
	
	private static void printPromptLabel() {
		System.out.print(promptLabel ? defaultLabel : secondaryPromptLabel);
	};
	
	private static void process(String cmd) {
		try {
			if (!promptLabel) {
				if(!cmd.equals("end")) {
					queries.add(cmd);
				} else {
					//TODO: flush all queries into cassandra
					//TODO: insert into columnfamily (name, notify_to, notify_on) values ('name', 'host:port', {'','',...});
					System.out.println("[name] : "+subscriberName);
					System.out.println("[host:port] : "+subscriberHostPort);
					System.out.println("[query] : "+ queries.toString());
					
					StringBuilder queryString = new StringBuilder();
					int n = queries.size();
					for(int i=0 ; i < n;i ++){
						String query = queries.get(i);
						String parsedQry = new UserQuery(query).getQuery();
						queryString.append("'").append(parsedQry.trim()).append("'");
						if(i < (n-1)) {
							queryString.append(",");
						}
					}
					
					String cqlQry = "insert into subscribers (id, name, notify_to, notify_on) values "
							+ "("
							+(++itemId)+", '"
							+subscriberName+"','"
							+subscriberHostPort+"',"
							+"["+queryString+"])";
					CassandraAdapter.printResultSet(cqlQry);
					
					promptLabel = true;
					queries = new ArrayList<String>();
				}
			} else if(cmd.equals("exit") || cmd.equals("quit")) {
				destroy();
			} else if (cmd.startsWith("sub")) {
				String[] cmdArr = cmd.split(" ");
				String option = cmdArr[1];
				if(cmdArr.length < 2){
					printUsage();
				} else if (option.equals("add")) {
					subscriberName = cmdArr[2];
					subscriberHostPort = cmdArr[3];
					
					if(option.equals("add")) {
						promptLabel = false;
						System.out.println("   -- enter 'end' to stop inserting queries --  ");
					}
				} else {
					CassandraAdapter.printResultSet("select * from subscribers"); 
				}
			} else if (cmd.startsWith("get:")) {
				String userQuery = cmd.substring(cmd.indexOf(":")+1);
				System.out.println("[qry]" + userQuery);
				
				String parsedQuery = new UserQuery(userQuery).getQuery();
				String cqlQry = "select * from items where " +parsedQuery.trim();
				System.out.println("[parsed] : "+parsedQuery);
				
				CassandraAdapter.printResultSet(cqlQry);
			} else {
				printUsage();
			}
		} catch (Exception e) {
			System.out.println("[ERROR]\n "+e.getMessage());
		}
	};
	
	private static void printUsage() {
		System.out.println("[Usage]");
		System.out.println(" sub name add|remove|list host:port");
		System.out.println(" exit|quit - exit prompt");
	}
	
	private static void destroy() {
		// TODO Auto-generated method stub
		CassandraAdapter.close();
		quitPrompt = false;
		sc.close();
	}

}