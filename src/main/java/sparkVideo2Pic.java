/**
 * Created by wuweiwei on 05/05/2017.
 */
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.bytedeco.javacpp.opencv_core.Mat;
import org.bytedeco.javacpp.opencv_core.RectVector;
import org.bytedeco.javacpp.opencv_core.Scalar;
import org.bytedeco.javacpp.opencv_imgcodecs.*;
import org.bytedeco.javacpp.opencv_imgproc.*;
import org.bytedeco.javacpp.opencv_objdetect.CascadeClassifier;
import org.bytedeco.javacv.*;

import javax.imageio.ImageIO;

import static java.lang.Thread.sleep;

public class sparkVideo2Pic {

    public static void video2Pic(String inputFile, String outputPath) throws IOException {

        System.out.println("filename is " + inputFile);
        System.out.println("output file path is " + outputPath);
        return;
//        // TODO need to load file in local memory FS, or find a memory Grabber API
//        FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(inputFile);
//
//        try {
//            grabber.start();
//        } catch (Exception e) {
//            // TODO exception print
//            e.printStackTrace();
//        }
//
//        long frameLength = grabber.getLengthInFrames();
//
//        // To grab frames per 0.5 seconds
//        for (int i=1; i < frameLength; i += grabber.getFrameRate()/2) {
//            Java2DFrameConverter converter = new Java2DFrameConverter();
//            grabber.setFrameNumber(i);
//
//            BufferedImage bi = converter.getBufferedImage(grabber.grab());
//            // TODO write to HDFS
//            ImageIO.write(bi, "jpg", new File(outputPath + "frame" + i + ".jpg"));
//        }
//
//        // To know number for each video
//        System.out.println("Video " + inputFile + " have " + frameLength + " frames.");
//
//        try {
//            grabber.stop();
//        } catch (FrameGrabber.Exception e) {
//            e.printStackTrace();
//        }


    }
    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setAppName("Video split");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        String outputPath = "hdfs://master:9000/output/";
        List<String> fileList = new ArrayList<String>();
        String filenameRDDPath = "hdfs://master:9000/filenamelist";

        try {
            FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] status = fs.listStatus(new Path("hdfs://master:9000/videos/"));
            for (FileStatus fileStat : status) {
                if (fileStat.isDirectory()) {

                } else {
                    fileList.add(fileStat.getPath().toString());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        // convert input path into RDD
        //JavaRDD<String> videoNamesRDD = jsc.textFile("hdfs://master:9000/videos/");
        JavaRDD<String> videoNamesRDD = jsc.parallelize(fileList,3);
        videoNamesRDD.saveAsTextFile(filenameRDDPath);
        JavaRDD<String> filenameRDD = jsc.textFile(filenameRDDPath);

        long startTime = System.currentTimeMillis();
        //System.out.println("####### Process files number is  " + videoNamesRDD.count());
        //System.out.println("####### partition is " + videoNamesRDD.getNumPartitions());
        // distribute video process
        filenameRDD.foreach(new VoidFunction<String>() {
            public void call (String videoFile) throws IOException {
                video2Pic(videoFile, outputPath);
            }
        });
        // get the process time
        long endTime   = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("####### Video decode take time " + totalTime/1000 + "s");

        // delete the filename list file
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            fs.delete(new Path(filenameRDDPath),true);
        } catch (IOException e) {
            e.printStackTrace();
        }

        while(true){
        }
        // done, stop spark
        //jsc.stop();

    }

}
