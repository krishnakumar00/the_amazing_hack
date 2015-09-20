package com.amazon.zama_edoc.client;

import java.util.ArrayList;
import java.util.Scanner;

public class Prompt {
	
	private static String defaultLabel = "# ";
	private static String secondaryPromptLabel = "    > ";
	private static Scanner sc = null;
	private static boolean quitPrompt = true;
	private static boolean promptLabel = true;
	private static String subscriberName = null;
	private static String subscriberHostPort = null;
	private static ArrayList<String> queries = new ArrayList<String>();

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
				if(!cmd.equals("no")) {
					queries.add(cmd);
				} else {
					//TODO: flush all queries into cassandra
					//TODO: insert into columnfamily subscribers (name, notify_to, notify_on) values ('<name>', '<host:port>',
					// {'price_value=454 and title='''','',...});
					System.out.println("Subscriber Name : "+subscriberName);
					System.out.println("Subscriber Host:Port : "+subscriberHostPort);
					System.out.println("Query To Be Inserted : "+ queries.toString());
					promptLabel = true;
					queries = new ArrayList<String>();
				}
			} else if(cmd.equals("exit") || cmd.equals("quit")) {
				destroy();
			} else if (cmd.startsWith("addsubscriber")) {
				String[] cmdArr = cmd.split(" ");
				if(cmdArr.length != 3){
					printUsage();
				} else {
					subscriberName = cmdArr[1];
					subscriberHostPort = cmdArr[2];
					promptLabel = false;
					System.out.println("   -- enter 'no' to stop inserting queries --  ");
				}
			} else {
				printUsage();
			}
		} catch (Exception e) {
			System.out.println("[ERROR]\n "+e.getMessage());
		}
	};
	
	private static void printUsage() {
		System.out.println("[Usage]");
		System.out.println(" addsubscriber name host:port");
		System.out.println(" exit|quit - exit prompt");
	}
	
	private static void destroy() {
		// TODO Auto-generated method stub
		quitPrompt = false;
		sc.close();
	}

}