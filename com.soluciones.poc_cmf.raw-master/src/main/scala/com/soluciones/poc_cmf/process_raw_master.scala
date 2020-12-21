package com.soluciones.poc_cmf



import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import java.util.Calendar
import com.soluciones.tables.master._
import com.soluciones.poc_cmf.datalake._
import com.soluciones.settings._

//import com.huemulsolutions.bigdata.tables._
//import com.huemulsolutions.bigdata.dataquality._


object process_raw_master {
  def main(args: Array[String]): Unit = {
    process_institucion.main(args)
    process_product.main(args)
    process_product_N1.main(args)
    process_product_N2.main(args)
    process_product_N3.main(args)
    process_product_N4.main(args)
    process_product_N5.main(args)


 
   
  }
}

