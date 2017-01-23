import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.io.FileNotFoundException; 
import java.io.IOException;

public class HDFSXorChecksum {

    public static void main ( String [] args ) throws Exception {

        // The system configuration
        Configuration conf = new Configuration();

        // Get an instance of the Filesystem
        FileSystem fs = FileSystem.get(conf);

        String path_name = "/class/s17419/lab1/bigdata";

        Path path = new Path(path_name);

        // The Input Data Stream to write into
        FSDataInputStream inputFile = fs.open(path);
        
        // Store the bytes read from file.
        byte[] fileContent = new byte[125];
        
        // The Xor Checksum of the bytes read from the file.
        byte xorCheckSum = (byte) 0;
        
        try{
            System.out.println("Rading bytes from the file now...");

            // Loop through all possible values within the offset range.
            for(int i=0; i<125; i++){
                // Compute file offset.
                long seekToPosition = 5000000000L + (i*8);
                // Start reading data with the offset.
                int result = inputFile.read(seekToPosition, fileContent, i, 1);
                
                if(result != -1){
                    if(i == 0){
                        xorCheckSum = fileContent[i];
                    }else{
                        // We are past the first byte reading. Start computing the xor checksum.
                        xorCheckSum = (byte) (xorCheckSum ^ fileContent[i]);
                    }
                    
                    System.out.println("Byte number " +i+ " is read as " + fileContent[i]);
                    System.out.println("XorCheckSum after byte " +i+ " is read = " + xorCheckSum);
                    System.out.println("--");
                }
            }
            
            System.out.println("After reading the data from the file, the 8-bit XorCheckSum is computed as : " + xorCheckSum);
            
        }
        catch (FileNotFoundException e){
            System.out.println("File not found" + e);
        }
        catch (IOException ioe) {
            System.out.println("Exception while reading file " + ioe);
        }
        
        // Close the file and the file system instance
        fs.close();
    }
}
