package cis5550.kvs;

import static cis5550.webserver.Server.*;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Coordinator extends cis5550.generic.Coordinator {
	// fields
	private static int portNo; // the port number on which to run the web server
	private static boolean ec2Config = false;
	

	public static void main(String[] args) { // accepts a single command-line argument
		// TODO 
		// initialize part
		System.out.println("KVS Coordinator starting...");
		init(args);
	}

	private static void parseCommand(String[] args) {

		if (args != null && args.length > 0) {
			try { 
				portNo = Integer.parseInt(args[0]);
			} catch (NumberFormatException e) { // TODO error handling for command-line argument
				System.out.println("Invalid Input. Expected a port number for the web server!");
				System.exit(1); // exit the program
			}
			ec2Config = args.length == 2 && args[1].toLowerCase().startsWith("y");
		} else {
			System.out.println("Invalid Input. Expected a port number for the web server!");
			System.exit(1); // exit the program
		}
	}
	
	/**
	 * Parses the command-line argument and passes it
	 * to the web server's port function.
	 */
	private static void init(String[] args) {
		// instantiate expiration scheduler
		ScheduledExecutorService sessionExpirationScheduler = Executors.newScheduledThreadPool(1);
        sessionExpirationScheduler.scheduleAtFixedRate(() -> {
        	try {
                Coordinator.expireEntries();
            } catch (Exception e) {
                e.printStackTrace(); // Log the exception
            }
        }, 0, 1, TimeUnit.SECONDS);
		// parse the command line 
		parseCommand(args);
		// pass port argument to web server
		port(portNo);
		if(ec2Config) {
			host("54.227.220.38", "keystore.jks", "secret");
		}
		// call registerRoutes
		registerRoutes();
		// define a GET route
		get("/", (req,res) -> { res.type("text/html"); return workerTable(); });
	}
}
