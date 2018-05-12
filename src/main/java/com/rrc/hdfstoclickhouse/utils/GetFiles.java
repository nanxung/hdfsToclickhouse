package com.rrc.hdfstoclickhouse.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;

public  class GetFiles {
    public static void main(String[] args){
        getFileList("/Users");
    }
    public static void getFileList(String dir){
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path(dir), false);
            while (iter.hasNext()){
                System.out.println(iter.next());
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
