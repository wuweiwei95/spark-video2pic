/**
 * Created by wuweiwei on 05/05/2017.
 */
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bytedeco.javacpp.opencv_core.Mat;
import org.bytedeco.javacpp.opencv_core.RectVector;
import org.bytedeco.javacpp.opencv_core.Scalar;
import org.bytedeco.javacpp.opencv_imgcodecs.*;
import org.bytedeco.javacpp.opencv_imgproc.*;
import org.bytedeco.javacpp.opencv_objdetect.CascadeClassifier;
import org.bytedeco.javacv.*;

import javax.imageio.ImageIO;

public class sparkVideo2Pic {

    public static void video2Pic(File inputFile, String outputPath) throws IOException {
        FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(inputFile);

        try {
            grabber.start();
        } catch (Exception e) {
            // TODO exception print
            e.printStackTrace();
        }

        long frameLength = grabber.getLengthInFrames();

        // To grab frames per 0.5 seconds
        for (int i=1; i < frameLength; i += grabber.getFrameRate()/2) {
            Java2DFrameConverter converter = new Java2DFrameConverter();
            grabber.setFrameNumber(i);

            BufferedImage bi = converter.getBufferedImage(grabber.grab());
            ImageIO.write(bi, "jpg", new File(outputPath + "frame" + i + ".jpg"));
        }

        // To know number for each video
        System.out.println("Video " + inputFile + " have " + frameLength + " frames.");

        try {
            grabber.stop();
        } catch (FrameGrabber.Exception e) {
            e.printStackTrace();
        }


    }
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Video split");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        String outputPath = "hdfs://master:9000/pic-out/";

        // convert input path into RDD, wholeTextFiles will read all the video file into memory
        JavaPairRDD<String, String> videosContentsRDD = jsc.wholeTextFiles("hdfs://master:9000/video-input/");
        long startTime = System.currentTimeMillis();
        System.out.println("Process files number is  " + videosContentsRDD.count());

        // distribute video split

        // get the process time
        long endTime   = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("Video decode take time " + totalTime/1000 + "s");

        // done, stop spark
        jsc.stop();

    }

}
