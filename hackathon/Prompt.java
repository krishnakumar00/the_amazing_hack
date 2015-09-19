package hackathon;

import java.util.Scanner;

public class Prompt {
	
	private static String promptLabel = "# ";
	private static Scanner sc = null;
	private static boolean quitPrompt = true;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// pre-load something..
		create();
	}

	private static void create() {
		// TODO Auto-generated method stub
		sc = new Scanner(System.in);
		while(quitPrompt) {
			System.out.print(promptLabel);
			process(sc.nextLine());
		}
	}
	
	private static void process(String cmd) {
		if(cmd.equals("exit") || cmd.equals("quit")) {
			destroy();
		} else if (cmd.equals("help")) {
			System.out.println("Print Usage\n");
		} else {
			System.out.println(" " + cmd);
		}
	};
	

	private static void destroy() {
		// TODO Auto-generated method stub
		quitPrompt = false;
		sc.close();
	}

}
