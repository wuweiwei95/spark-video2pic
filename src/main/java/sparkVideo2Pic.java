/**
 * Created by wuweiwei on 05/05/2017.
 */
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

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
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Video split");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        String outputPath = "hdfs://master:9000/output/";

        // convert input path into RDD, wholeTextFiles will read all the video file into memory
        JavaRDD<String> videoNamesRDD = jsc.textFile("hdfs://master:9000/videos/");
        long startTime = System.currentTimeMillis();
        System.out.println("Process files number is  " + videoNamesRDD.count());

        // distribute video process
        videoNamesRDD.foreach(new VoidFunction<String>() {
            public void call (String videoFile) throws IOException {
                video2Pic(videoFile, outputPath);
            }
        });
        // get the process time
        long endTime   = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("Video decode take time " + totalTime/1000 + "s");

        // done, stop spark
        jsc.stop();

    }

}
