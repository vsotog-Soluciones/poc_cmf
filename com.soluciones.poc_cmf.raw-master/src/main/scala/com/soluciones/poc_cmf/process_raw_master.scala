package com.soluciones.poc_cmf



import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import java.util.Calendar
import com.soluciones.tables.master._
import com.soluciones.poc_cmf.datalake._
import com.soluciones.settings._

//import com.huemulsolutions.bigdata.tables._
//import com.huemulsolutions.bigdata.dataquality._


object process_mensual {
  def main(args: Array[String]): Unit = {
    process_activosbanco1_mes.main(args)
    process_activosbanco2_mes.main(args)
    process_product.main(args)
    process_productN1.main(args)
    process_productN2.main(args)
    process_productN3.main(args)
    process_productN4.main(args)
    process_productN5.main(args)
    process_institucion.main(args)
 
   
  }
}

