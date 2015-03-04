package com.tfidf.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.List;

public class LocalToHDFSCopier {
    public void copyToHDFS(List<String> fileNames,String hdfsPath,FileSystem hdfs) throws IOException
    {
        //1. Get the instance of COnfiguration
        final Configuration conf = hdfs.getConf();
        for(final String filename:fileNames){
            //2. Create an InputStream to read the data from local file
            InputStream inputStream = new BufferedInputStream(new FileInputStream(filename));
            //4. Open a OutputStream to write the data, this can be obtained from the FileSytem
            OutputStream outputStream = hdfs.create(new Path(hdfsPath+"/"+filename), new Progressable() {
                @Override
                public void progress() {
                    System.out.println("Copying file: "+filename);
                }
            });
            try
            {
                IOUtils.copyBytes(inputStream,outputStream,conf,true);
                //java.nio.file.Path localPath = FileSystems.getDefault().getPath(filename);
                //Files.delete(localPath);
            }
            finally
            {
                IOUtils.closeStream(inputStream);
                IOUtils.closeStream(outputStream);
            }

        }//for ends

    }
}
