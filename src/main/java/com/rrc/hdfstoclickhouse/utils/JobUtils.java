package com.rrc.hdfstoclickhouse.utils;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class JobUtils {
    private static Job job;
    private static Path input;
    private static Path output;

    public static void setConf(
            Configuration c,
            String name,
            String in,
            String out,Class cc) throws IOException{
        job=Job.getInstance(c,name);
        job.setJarByClass(cc);
        input=new Path(in);
        output=new Path(out);
    }

    public static void setMapper(
            Class<? extends Mapper> mClass,
            Class<? extends WritableComparable> keyClass,
            Class<? extends Writable> valueClass,
            Class<? extends InputFormat> inClass)
            throws IllegalAccessException,
            InstantiationException, IOException,
            NoSuchMethodException,
            InvocationTargetException{

        job.setMapperClass(mClass);
        job.setMapOutputKeyClass(keyClass);
        job.setMapOutputValueClass(valueClass);
        job.setInputFormatClass(inClass);

        Method method=
                inClass.getMethod(
                        "addInputPath",Job.class,Path.class);
        method.invoke(null,job,input);

        //FileInputFormat.addInputPath(job,input);

    }

    public static void setReducer(
            Class<? extends Reducer> rClass,
            Class<? extends WritableComparable> keyClass,
            Class<? extends WritableComparable> valueClass,
            Class<? extends OutputFormat> outClass){

        job.setReducerClass(rClass);
        job.setOutputKeyClass(keyClass);
        job.setOutputValueClass(valueClass);
        job.setOutputFormatClass(outClass);
        FileOutputFormat.setOutputPath(job,output);
    }

    public static void
    setCombiner(
            Class<? extends Reducer> c){
        if(c!=null) {
            job.setCombinerClass(c);
        }
    }

    public static int commit() throws InterruptedException, IOException, ClassNotFoundException{
        return job.waitForCompletion(true)?0:1;
    }
}
