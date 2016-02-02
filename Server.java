

import java.net.*;
import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.Charset;
import java.util.*;

public class Server {

	private static int sPort;   //The server will be listening on this port number
	private static final String suffix = ".txt";
	private static int number = 0;
	private static String[] chunkNames;
	private static String configurationFilePath = "/Users/Yunze/Documents/workspace/P2P/Configuration/Configuration.txt";
	private static String name = "/Users/Yunze/Documents/workspace/P2P/test/Demo_data.zip";
	private static String fileName;
	private static List<Handler> thread_list = new ArrayList<Handler>();   //list of all threads the server has started

	public static void main(String[] args) throws Exception {
		sPort = loadConfiguration(configurationFilePath);
		System.out.println("The server is running."); 
		System.out.println("My listening port number is "+ sPort +"."); 
        ServerSocket listener = new ServerSocket(sPort);
		int peerNum = 1;
		long size = 100 * 1024; //size is 100kb = 100*1024 bytes
		chunkNames = slideFile(name,size);
        	try {    		
            		while(true) {
                		new Handler(listener.accept(),peerNum).start();
                		peerNum++;
            			}
        	} finally {
            		listener.close();
        	} 
 
    	}
	
	//read the configuration file and load the file owner's port number port 
	public static int loadConfiguration(String path) throws Exception {
		File file = new File(path);
		//test whether the upload file is error
    	if(!file.exists()||!file.isFile()){
    		throw new Exception("The file is not exists!");
    	}
        //start to read the configuration file
    	BufferedReader reader = new BufferedReader(new FileReader(file));
    	int result = 0;
    	String readLine;
		while((readLine = reader.readLine()) != null){
			String[] tmp = readLine.split("  ");;
			for(String s :tmp){
				if(tmp[0].equalsIgnoreCase("FileOwner")){
					//this line is for file owner
					result = Integer.parseInt(tmp[1]);
					System.out.println("configuration file is read, the port number I use is "+result);
					break;
				}
			}
			break;
		}
		reader.close();
		return result;	
	}

	//cut the file into chunks, each has 100kb length
    public static String[] slideFile(String name, long size) throws Exception {
    	File file = new File(name);
    	fileName = file.getName();
    	//test whether the upload file is error
    	if(!file.exists()||!file.isFile()){
    		throw new Exception("The file is not exists!");
    	}
    	//get the original file's parentFile and length
    	File parentFile = file.getParentFile();
    	long fileLength = file.length(); 
    	System.out.println("The original file is "+fileLength+" bytes");
    	if(size < 0){
    		throw new Exception("The silde size must be positive!");
    	}
    	//calculate the total number of chunks after cut
    	number = fileLength % size != 0 ? (int) (fileLength/size + 1): (int) (fileLength/size);
    	System.out.println("The original file will divided into "+ number +" chunks");
    	String[] chunkNames = new String[number];
    	//read the original file from local address
    	FileInputStream fileIn = new FileInputStream(file);
    	long end = 0;
    	int begin = 0;
    	//start to cut file into chunks
    	for(int i = 1; i <= number; i++){
    		File chunk = new File(parentFile, "chunk "+ i + suffix);
    		//output the chunk after cut
    		FileOutputStream fileOut = new FileOutputStream(chunk);
    		System.out.println("The chunk "+ i +" is created");
    		end += size;
    		end = (end>fileLength) ? fileLength : end;
    		//read from the input to output
    		for( ; begin < end; begin++){
    			fileOut.write(fileIn.read());
    		}
    		fileOut.close();
    		chunkNames[i-1] = chunk.getAbsolutePath();
    	}
    	fileIn.close();
    	System.out.println("The cut process is done.");
    	
    	return chunkNames;
    }

//    public static void allocateFile() throws Exception{
//    	
//    }
    
    
    //Handler is responsible for communicate with different peers to send different chunks 
   	private static class Handler extends Thread {
        	private Socket connection;
        	private ObjectInputStream in;	//stream read from the socket
        	private ObjectOutputStream out;    //stream write to the socket     	
        	private int no;		//The index number of the peer

        	public Handler(Socket connection, int no) {
            	this.connection = connection;
	    		this.no = no;
	    		thread_list.add(this);
	    		int peerPortNumber = connection.getPort();
	    		System.out.println("Connected with peer " +no+ ", it's port number is "+peerPortNumber+".");
        	}

        public void run() {
 		try{
			//initialize Input and Output streams
			out = new ObjectOutputStream(connection.getOutputStream());
			out.flush();
			in = new ObjectInputStream(connection.getInputStream());    
			//handshaking, the connection is set up 
			//send the file name to peer
			out.writeObject(fileName);
			out.flush();
			//send the total chunk names to peer
        	out.writeObject(number);
        	out.flush();
			//allocate chunks to this peer
        		for(int i = no; i <= number; i+=5){
        			//send chunk i to peer peerNum
        			File chunk = new File(chunkNames[i-1]);
        			sendFile(chunk, i);
        		}
		}
 	
		catch(IOException ioException){
			System.out.println("Disconnect with peer " + no);
		}
//		finally{
//			//Close connections
//				try{
//					in.close();
//					out.close();
//					connection.close();
//					}
//			catch(IOException ioException){
//				System.out.println("Disconnect with peer " + no);
//				}
//			}
        }
        
      //send each chunk to peers
        public void sendFile(File file, int i) {
	    try{
	    	out.writeObject(i);
	    	out.flush();
	    	out.writeObject(file);
	    	out.flush();
	    	System.out.println("send chunk " + i + " to peer" + no);
	    }
	    catch(IOException ioException){
	    	ioException.printStackTrace();
	    }
       } 
        
    }

}

    	
    	
    	
