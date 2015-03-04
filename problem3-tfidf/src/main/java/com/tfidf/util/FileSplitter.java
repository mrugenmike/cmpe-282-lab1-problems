package com.tfidf.util;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class FileSplitter {

    public List<String> splitFile(String filePath, int splitLines, FileSystem fs){
        final List<String> filenamesCreated= new ArrayList<String>();
        try{
            final Path pt=new Path(filePath);
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            int totalLinesInFile = 0;
            while (br.readLine() != null){
                totalLinesInFile++;
            }

            System.out.println("Total Lines:"+totalLinesInFile);
            final double totalFileSplits = (totalLinesInFile / splitLines);
            int noOfFilesToCreate = 0;
            if((int)totalFileSplits==totalFileSplits){
                noOfFilesToCreate = (int) totalFileSplits;
            }else{
                noOfFilesToCreate = totalLinesInFile+1;
            }
            System.out.println("Total New Files created will be:"+noOfFilesToCreate);

            FileInputStream fstream = new FileInputStream(filePath);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader newFileBufferedReader = new BufferedReader(new InputStreamReader(in));

            for(int index=1;index<=noOfFilesToCreate;index++){
                String fileName = pt.getName() + "_" + index + ".txt";
                FileWriter fstream1 = new FileWriter(fileName);     // Destination File Location
                BufferedWriter out = new BufferedWriter(fstream1);
                for (int j=1;j<=totalLinesInFile;j++)
                {
                    String strLine = newFileBufferedReader.readLine();
                    if (strLine!= null)
                    {
                        out.write(strLine);
                        if(j!=splitLines)
                        {
                        out.newLine();
                        }
                    }
                }
                filenamesCreated.add(fileName);
                out.close();
            }
            in.close();
            return filenamesCreated;

        }catch(Exception e){
            e.printStackTrace();
        }
    return filenamesCreated;
    }//
}
