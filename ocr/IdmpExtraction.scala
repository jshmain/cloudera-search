/** 
 * @author Jeff Shmain
 * @author Vartika Singh
 * /

package com.cloudera.sa.OCR

import org.bytedeco.javacpp.lept._;
import org.bytedeco.javacpp.tesseract._;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteBuffer
import java.awt.Image
import java.awt.image.RenderedImage;
import java.io.File;
import scala.collection.mutable.StringBuilder
import collection.JavaConversions._
import java.io.IOException;
import java.util.List;
import javax.imageio.ImageIO;
import org.ghost4j.analyzer.AnalysisItem;
import org.ghost4j.analyzer.FontAnalyzer;
import org.ghost4j.document.PDFDocument;
import org.ghost4j.renderer.SimpleRenderer;
import org.bytedeco.javacpp._;
import java.io._
import org.apache.spark.{Logging, SerializableWritable, SparkConf, SparkContext}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

object IdmpExtraction {
  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName("IDMP Processor")
    val sc = new SparkContext(conf)

    /** Read in PDFs into the RDD */
    val files = sc.binaryFiles ("hdfs://nameservice1/data/raw")
    files.map(convertFunc(_)).count
  }


  /** Populate the HBase table
    *	@param fileName This corresponds to the rowID in HBase
    * 	@param lines The parsed output.
    *	dataformat: binaryPDF:PortableDataStream
    */
  def populateHbase (
                      fileName:String,
                      lines: String,
                      pdf:org.apache.spark.input.PortableDataStream) : Unit =
  {
    /** Configure and open a HBase connection */
    val conf = HBaseConfiguration.create()
    val conn=  ConnectionFactory.createConnection( conf );
    val mddsTbl = _conn.getTable( TableName.valueOf( "mdds" ));
    val cf = "info"
    val put = new Put( Bytes.toBytes( fileName ))

    /**
     *	Extract Fields here using Regexes
     *	Create Put objects and send to hbase
     */
    val aAndCP = """(?s)(?m).*\d\d\d\d\d-\d\d\d\d(.*)\nRe: (\w\d\d\d\d\d\d).*""".r
    val approvedP = """(?s)(?m).*(You may, therefore, market the device, subject to the general controls provisions of the Act).*""".r

    lines match {
      case
        aAndCP( addr, casenum ) => put.add( Bytes.toBytes( cf ), Bytes.toBytes( "submitter_info" ), Bytes.toBytes( addr ) ).add( Bytes.toBytes( cf ), Bytes.toBytes( "case_num" ), Bytes.toBytes( casenum ))
      case _ => println( "did not match a regex" )
    }

    lines match {
      case
        approvedP( approved ) => put.add( Bytes.toBytes( cf ), Bytes.toBytes( "approved" ), Bytes.toBytes( "yes" ))
      case _ => put.add( Bytes.toBytes( cf ), Bytes.toBytes( "approved" ), Bytes.toBytes( "no" ))
    }

    lines.split("\n").foreach {

      val regNumRegex = """Regulation Number:\s+(.+)""".r
      val regNameRegex = """Regulation Name:\s+(.+)""".r
      val regClassRegex = """Regulatory Class:\s+(.+)""".r
      val productCodeRegex = """Product Code:\s+(.+)""".r
      val datedRegex = """Dated:\s+(\w{3,10}\s+\d{1,2},\s+\d{4}).*""".r
      val receivedRegex = """Received:\s+(\w{3,10}\s+\d{1,2},\s+\d{4}).*""".r
      val deviceNameRegex = """Trade/Device Name:\s+(.+)""".r

      _ match {
        case regNumRegex( regNum ) => put.add( Bytes.toBytes( cf ), Bytes.toBytes( "reg_num" ), Bytes.toBytes( regNum ))
        case regNameRegex(regName) => put.add(Bytes.toBytes( cf ), Bytes.toBytes( "reg_name" ), Bytes.toBytes( regName ))
        case regClassRegex( regClass ) => put.add( Bytes.toBytes( cf ), Bytes.toBytes( "reg_class" ), Bytes.toBytes( regClass ))
        case productCodeRegex( productCode ) => put.add( Bytes.toBytes( cf ), Bytes.toBytes( "product_code" ), Bytes.toBytes( productCode ))
        case datedRegex( dated ) => put.add( Bytes.toBytes( cf ), Bytes.toBytes( "dated" ), Bytes.toBytes( dated ))
        case receivedRegex( received ) => put.add( Bytes.toBytes( cf ), Bytes.toBytes( "received" ), Bytes.toBytes( received ))
        case deviceNameRegex( deviceName ) => put.add( Bytes.toBytes( cf ), Bytes.toBytes( "device_name" ), Bytes.toBytes( deviceName ))

        case _ => print( "" )
      }
    }
    put.add( Bytes.toBytes( cf ), Bytes.toBytes( "text" ), Bytes.toBytes( lines ))
    val pdfBytes = pdf.toArray.clone
    put.add(Bytes.toBytes( "obj" ), Bytes.toBytes( "pdf" ), pdfBytes )

    mddsTbl.put( put )
    mddsTbl.close
    conn.close
  }

  /** Method to convert a PDF document to images and hence OCR
    * @param PDF File(s) to process
    */
  def convertFunc (
                    file: (String, org.apache.spark.input.PortableDataStream)
                    ) : Unit  =
  {
    /** Render the PDF into a list of images with 300 dpi resolution
      * One image per PDF page, a PDF document may have multiple pages
      */
    val document: PDFDocument = new PDFDocument( );
    document.load( file._2.open )
    file._2.close
    val renderer :SimpleRenderer = new SimpleRenderer( )
    renderer.setResolution( 300 )
    val images:List[Image] = renderer.render( document )

    /**  Iterate through the image list and extract OCR
      * using Tesseract API.
      */
    var r:StringBuilder = new StringBuilder
    images.toList.foreach{ x=>
      val imageByteStream = new ByteArrayOutputStream( )
      ImageIO.write(
        x.asInstanceOf[RenderedImage], "png", imageByteStream )
      val pix: PIX = pixReadMem(
        ByteBuffer.wrap( imageByteStream.toByteArray( ) ).array( ),
        ByteBuffer.wrap( imageByteStream.toByteArray( ) ).capacity( )
      )
      val api: TessBaseAPI = new TessBaseAPI( )
      /** We assume the documents are in English here, hence \”eng\” */
      api.Init( null, "eng" )
      api.SetImage(pix)
      r.append(api.GetUTF8Text().getString())
      imageByteStream.close
      pixDestroy(pix)
      api.End
    }

    /** Write the generated data into HBase */
    populateHbase( file._1, r.toString( ), file._2 )
  }
}
