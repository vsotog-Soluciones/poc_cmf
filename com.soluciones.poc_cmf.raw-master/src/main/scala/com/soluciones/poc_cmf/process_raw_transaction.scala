package com.soluciones.poc_cmf



import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import java.util.Calendar
import com.soluciones.tables.master._
import com.soluciones.poc_cmf.datalake._
import com.soluciones.settings._

//import com.huemulsolutions.bigdata.tables._
//import com.huemulsolutions.bigdata.dataquality._


object process_raw_transaction {
  def main(args: Array[String]): Unit = {
    process_activosbanco.main(args)
    //process_product.main(args)

 
   
  }
}

