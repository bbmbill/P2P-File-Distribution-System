

import java.net.*;
import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.Charset;
import java.util.*;

public class Peer5 {
	private static Socket fileOwnerSocket;           //socket connect to the file owner
	private static int fileOwnerPort = 8000;
	private static int localPort = 8005;
	private static int listeningPort = 7005;
	private static int downloadListeningPort = 7004;
	private static ObjectOutputStream out;         //stream write to the fileOwnerSocket
    private static ObjectInputStream in;          //stream read from the fileOwnerSocket
	private static String message;                //message to close this peer
	private static String fileName;               //the original file name
	private static String localAddress = "/Users/Yunze/Documents/workspace/P2P/peer5/";
	private static String configurationFilePath = "/Users/Yunze/Documents/workspace/P2P/Configuration/Configuration.txt";
	private static File summary = new File(localAddress+"summary.txt");
	private static List<Integer> want_chunk_list = new ArrayList<Integer>();
	private static List<Integer> got_chunk_list = new ArrayList<Integer>();
	private static ServerSocket uploadSocket;
	private static Socket downloadSocket;
	private static boolean finishReceive = false;
	
	void run()
	{
		try{
			//read the configuration file
			try{readConfiguration();}
			catch(Exception e){}
			//create a fileOwnerSocket, use for connect with file owner
			fileOwnerSocket = new Socket();
			//bind this socket with port number 8005
			fileOwnerSocket.bind(new InetSocketAddress("localhost", localPort));
			//connect this socket to the server
			fileOwnerSocket.connect(new InetSocketAddress("localhost", fileOwnerPort));
			//initialize inputStream and outputStream
			out = new ObjectOutputStream(fileOwnerSocket.getOutputStream());
			out.flush();
			in = new ObjectInputStream(fileOwnerSocket.getInputStream());
			//handshaking is finish, the connection is set up
			System.out.println("Connected to localhost in port "+fileOwnerPort+".");
			System.out.println("My own port number for file owner is "+localPort+".");
			new receiveChunksThread();   //listen to the file owner, print everything received
			new listeningUploadThread();  //listen to upload neighbor who wants to send request to me
			new connectingDownloadThread();  //connect to my download neighbor to request for chunks
			
			//get Input from standard input
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
			while(true)
				{
				//close the connection of this peer
				message = bufferedReader.readLine();
				if (message != null && message.length() == 0)
					{
					//close the connection
					in.close();
					out.close();
					fileOwnerSocket.close();
					System.out.println("this client is closed.");
					break;
					}
				
				}		
			}
		catch (ConnectException e) {
    			System.err.println("Connection refused. You need to initiate a server first.");
			} 
		catch(UnknownHostException unknownHost){
			System.err.println("You are trying to connect to an unknown host!");
			}	
		catch(IOException ioException){
			ioException.printStackTrace();
			}
		}
	
	//read the configuration file
	private static void readConfiguration() throws Exception{
				File file = new File(configurationFilePath);
				//test whether the upload file is error
			    if(!file.exists()||!file.isFile()){
			   		throw new Exception("The file is not exists!");
			   	}
			       //start to read the configuration file
			   	BufferedReader reader = new BufferedReader(new FileReader(file));
		    	String readLine;
			    while((readLine = reader.readLine()) != null){
					String[] tmp = readLine.split("  ");;
						if(tmp[0].equalsIgnoreCase("FileOwner")){
							//this line is for file owner
							fileOwnerPort = Integer.parseInt(tmp[1]);
							System.out.println("configuration file is read, the port number of file owner is "+fileOwnerPort);
						}
						else if(tmp[0].equalsIgnoreCase("5")){
							//this line is for peer1
							localPort = Integer.parseInt(tmp[1]);
							System.out.println("configuration file is read, my local port number of file owner is "+localPort);
							listeningPort = Integer.parseInt(tmp[2]);
							System.out.println("configuration file is read, the port number I use for listening is "+listeningPort);
							downloadListeningPort  = Integer.parseInt(tmp[3]);
							System.out.println("configuration file is read, the port number my download neighbor use for listening is "+downloadListeningPort);
							break;
						}
						
					}
		    	reader.close();
		    	}
	
	//the receiving thread to receive chunks from file owner
	private static class receiveChunksThread extends Thread{
		
		public receiveChunksThread() {
			start();
		}
		@Override
		public void run() {
		try{
			//Receive file name from file owner
			fileName = (String)in.readObject();
			//Receive total number of chunks from file owner
			int chunkNumber = (int)in.readObject();
			//fill the chunk_list with all chunks it need to receive
			for(int i=1; i<=chunkNumber; i++){
				want_chunk_list.add(i);
			}
			while(finishReceive == false)
				{
				//Receive the chunks from file owner
				int chunkIndex = (int)in.readObject();
				File chunk = (File)in.readObject();
				long chunkLength = chunk.length();
				String chunkName = chunk.getName();
				System.out.println(chunkName+" is received, the length is " +chunkLength+ "bytes.");
				FileInputStream chunkIn = new FileInputStream(chunk);
				FileOutputStream chunkOut = new FileOutputStream(localAddress+ chunkName);
				for(int i = 0 ; i < chunkLength; i++){
					chunkOut.write(chunkIn.read());
			    		}
				chunkOut.close();
				chunkIn.close();
				//show the message to the user
				System.out.println(chunkName +" is saved.");
		        //remove this chunk from want_chunk_list
				want_chunk_list.remove((Object)chunkIndex);
				//add this chunk to got_chunk_list
				got_chunk_list.add(chunkIndex);
				//create a summary file after receive file from file owner
				FileOutputStream summaryOut =new FileOutputStream(summary,true);
				summaryOut.write(chunkName.getBytes(Charset.forName("UTF-8")));
				summaryOut.write(" From file owner.".getBytes(Charset.forName("UTF-8")));
				summaryOut.write('\n');
				summaryOut.close();
				}
					
			}
		catch ( ClassNotFoundException e ){
    		System.err.println("Class not found");
			}
		catch(IOException ioException){
			ioException.printStackTrace();
			}
		}
	}
	
	//the thread to listen for upload neighbor to provide chunks
	private static class listeningUploadThread extends Thread{
		private static ObjectOutputStream uploadOut;         //stream write to the uploadSocket
	    private static ObjectInputStream uploadIn;          //stream read from the uploadSocket
		
		public listeningUploadThread(){
			start();
		}
		@Override
		public void run(){
			try{
				//create a uploadSocket, use for listening upload neighbor
				uploadSocket = new ServerSocket(listeningPort);
				//Wait for connection
				System.out.println("peer 5 Waiting for connection");
				Socket connection = uploadSocket.accept();
				//agree connection, initialize Input and Output streams
				uploadOut = new ObjectOutputStream(connection.getOutputStream());
				uploadOut.flush();
				uploadIn = new ObjectInputStream(connection.getInputStream());
				System.out.println("Connected to upload neighbor.");
				
				while(true)
				{
				//receive the request for sending my got_chunk_list, if not, just wait
				int request = (int)uploadIn.readObject();
				
				if(request == 1){ //this request is a send_chunk_list request
				System.out.println("Receive request from my upload neighbor.");
				//send my got_chunk_list to upload neighbor
				uploadOut.writeInt(got_chunk_list.size());
				uploadOut.flush();
				System.out.println("send my chunk ID list:"+ got_chunk_list+"to upload neighbor.");
				for(int i = 0; i< got_chunk_list.size();i++){
				uploadOut.writeInt(got_chunk_list.get(i));
				uploadOut.flush();
				}
				
				//receive the chunkIndex list that my upload neighbor wants
				int number = uploadIn.readInt();
				List<Integer> uploadWants = new ArrayList<Integer>();
				for(int i=0; i< number; i++){
					uploadWants.add(uploadIn.readInt());
				}
				System.out.println("Receive chunk ID list:"+uploadWants+" that my upload neighbor wants.");
				for(Integer indexUploadWants:uploadWants){
					for (Integer indexHas:got_chunk_list){
						if(indexUploadWants.intValue() == indexHas.intValue()){
							//if the one my upload neighbor wants I really have, send each chunks to him
							File chunk = new File(localAddress+"chunk "+indexUploadWants.intValue()+".txt");
							uploadOut.writeObject(indexUploadWants.intValue());
							uploadOut.flush();
							uploadOut.writeObject(chunk);
							uploadOut.flush();
							System.out.println("Send chunk "+ indexUploadWants.intValue() +" to my upload neighbor.");
							}
						}
					}
				}
				else if(request == 0){ //this request is a close request
					break;
				}
				}
			}
			catch(ClassNotFoundException classnot){
				System.err.println("Data received in unknown format");
			}
			catch(IOException ioException){
				ioException.printStackTrace();
				}
			finally{
				//Close connections
				try{
					uploadIn.close();
					uploadOut.close();
					uploadSocket.close();
					System.out.println("Connection with upload neighbor is closed.");
				}
				catch(IOException ioException){
					ioException.printStackTrace();
				}
			}	
		}
	}
	
	//the thread to connect to download neighbor to ask for chunks
	private static class connectingDownloadThread extends Thread{
	    private static ObjectOutputStream downloadOut;         //stream write to the downloadSocket
	    private static ObjectInputStream downloadIn;          //stream read from the downloadSocket
		
		public connectingDownloadThread(){
			start();
		}
		public Socket connection(){
			try{
				downloadSocket = new Socket("localhost",downloadListeningPort);
				return downloadSocket;
			}
//			catch ( ClassNotFoundException e ) {
//        		System.err.println("Class not found");
//			} 
			catch(UnknownHostException unknownHost){
				System.err.println("You are trying to connect to an unknown host!");
				return downloadSocket;
			}
			catch(IOException ioException){
				System.err.println("No peer in such port number, 3 seconds later try again.");
				return downloadSocket;
			}
		}
		@Override
		public void run(){
			try{
				Socket downloadSocket = connection();
				while(downloadSocket == null){
				try{
					Thread.sleep(3000);
					downloadSocket = connection();
					}
				catch( InterruptedException e){ 
					}
				}
				//create client's socket to download neighbor
				downloadOut= new ObjectOutputStream(downloadSocket.getOutputStream());
				downloadOut.flush();
				downloadIn = new ObjectInputStream(downloadSocket.getInputStream());
				//set up connection with download neighbor
				System.out.println("Connected to donwload neighbor in port "+downloadListeningPort+".");
				
				while(finishReceive == false){ // when this peer not received all chunks
				try{
					//wait for 2 seconds
					Thread.sleep(2000);
				}
				catch( InterruptedException e){ 
				}
				//then send request to get the get_chunk_list from download neighbor
				int request = 1;
				System.out.println("Send request to download neighbor.");
				downloadOut.writeObject(request);
				downloadOut.flush();
				//receive chunk_list from download neighbor
				int number = downloadIn.readInt();
				List<Integer> downloadHas = new ArrayList<Integer>();
				for(int i=0; i<number; i++){
				downloadHas.add(downloadIn.readInt());
				}
				System.out.println("Receive chunk ID list:"+downloadHas+" that my download neighbor has.");
				//compare with chunks that I already have, record the chunks that I want from download neighbor has
				List<Integer> chunksWant = new ArrayList<Integer>();
				for(Integer indexDownloadHas: downloadHas){
					for(Integer indexWantChunk: want_chunk_list){
						if(indexDownloadHas.intValue() == indexWantChunk.intValue()){
							//my download neighbor has the chunk that I want
							chunksWant.add(indexDownloadHas.intValue());
						}	//my download don't have the one I want, send an empty list back	
					}
				}
				//send the chunk list I pick to my download neighbor
				downloadOut.writeInt(chunksWant.size());
				downloadOut.flush();
				System.out.println("send chunk ID list:"+chunksWant+" that I want.");
				for(int i=0; i< chunksWant.size(); i++){
				downloadOut.writeInt(chunksWant.get(i));
				downloadOut.flush();
				}
				//receive the chunks my download neighbor send to me
				for(int i=0; i<chunksWant.size(); i++){
					int chunkIndex = (int)downloadIn.readObject();
					File chunk = (File)downloadIn.readObject();
					String chunkName = chunk.getName();
					long chunkLength = chunk.length();
					System.out.println("Receive "+chunkName+" from my download neighbor, the length is "+chunkLength+". Thank you.");
					FileInputStream chunkIn = new FileInputStream(chunk);
					FileOutputStream chunkOut = new FileOutputStream(localAddress+ chunkName);
					for(int m = 0 ; m < chunkLength; m++){
						chunkOut.write(chunkIn.read());
				    		}
					chunkOut.close();
					chunkIn.close();
					System.out.println(chunkName+" is saved.");
					got_chunk_list.add(chunkIndex);
					want_chunk_list.remove((Object)chunkIndex);
					//add this new chunk information in summary file
					FileOutputStream summaryOut =new FileOutputStream(summary,true);
					summaryOut.write(chunkName.getBytes(Charset.forName("UTF-8")));
					summaryOut.write(" From download neighbor.".getBytes(Charset.forName("UTF-8")));
					summaryOut.write('\n');
					summaryOut.close();	
					
					}
					//check whether all chunks has received
					if(want_chunk_list.isEmpty()){
						finishReceive = true;
						}
					}
					System.out.println("All chunks have received! Congratulation!");
					//send close request
					int close = 0;
					downloadOut.writeObject(close);
					downloadOut.flush();	
					//build all files together
					File finalFile = new File(localAddress+fileName);
					for(int i=1; i<=got_chunk_list.size(); i++){
						//write each chunk's data
						File chunk = new File(localAddress+"chunk "+i+".txt");
						FileInputStream fileIn = new FileInputStream(chunk);
						FileOutputStream fileOut = new FileOutputStream(finalFile, true);
						for(int m=0; m< chunk.length();m++){
							fileOut.write(fileIn.read());
						}
						//this chunk is finish
						System.out.println(chunk.getName()+" is added to final file.");
						fileIn.close();
						fileOut.close();
					}
					//finish build up
					System.out.println("successfully build up the final file, Good-bye.");
					
			}
			catch ( ClassNotFoundException e ) {
            		System.err.println("Class not found");
        	} 
			catch(UnknownHostException unknownHost){
				System.err.println("You are trying to connect to an unknown host!");
			}
			catch(IOException ioException){
				ioException.printStackTrace();
			}
			finally{
				//Close connections
				try{
					downloadIn.close();
					downloadOut.close();
					downloadSocket.close();
					System.out.println("Socket with download neighbor is closed.");
				}
				catch(IOException ioException){
					ioException.printStackTrace();
				}
			}	
		}	
	}
	
	//main method
	public static void main(String args[])
	{
		Peer5 peer = new Peer5();
		peer.run();      //start the peer
	}

}

