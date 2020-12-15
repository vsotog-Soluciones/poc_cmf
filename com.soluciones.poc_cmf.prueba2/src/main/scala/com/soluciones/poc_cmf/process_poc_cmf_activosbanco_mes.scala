package com.soluciones.poc_cmf



import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import java.util.Calendar
import com.soluciones.tables.master._
import com.soluciones.poc_cmf.datalake._
import com.soluciones.settings._

//import com.huemulsolutions.bigdata.tables._
//import com.huemulsolutions.bigdata.dataquality._


object process_institucion_mes {
  
  /**
   * Este codigo se ejecuta cuando se llama el JAR desde spark2-submit. el codigo esta preparado para hacer reprocesamiento masivo.
  */
  def main(args : Array[String]) {
    //Creacion API
    val huemulBigDataGov  = new huemul_BigDataGovernance(s"Masterizacion tabla tbl_poc_cmf_institucion_mes - ${this.getClass.getSimpleName}", args, globalSettings.Global)
    
    /*************** PARAMETROS **********************/
    var param_ano = huemulBigDataGov.arguments.GetValue("ano", null, "Debe especificar el parametro año, ej: ano=2017").toInt
    var param_mes = huemulBigDataGov.arguments.GetValue("mes", null, "Debe especificar el parametro mes, ej: mes=12").toInt
     
    val param_dia = 1
    val param_numMeses = huemulBigDataGov.arguments.GetValue("num_meses", "1").toInt

    /*************** CICLO REPROCESO MASIVO **********************/
    var i: Int = 1
    val Fecha = huemulBigDataGov.setDateTime(param_ano, param_mes, param_dia, 0, 0, 0)
    
    while (i <= param_numMeses) {
      param_ano = huemulBigDataGov.getYear(Fecha)
      param_mes = huemulBigDataGov.getMonth(Fecha)
      println(s"Procesando Año $param_ano, Mes $param_mes ($i de $param_numMeses)")
      
      //Ejecuta codigo
      val finControl = process_master(huemulBigDataGov, null, param_ano, param_mes)
      
      if (finControl.Control_Error.IsOK())
        i+=1
      else {
        println(s"ERROR Procesando Año $param_ano, Mes $param_mes ($i de $param_numMeses)")
        i = param_numMeses + 1
      }
        
      Fecha.add(Calendar.MONTH, 1)      
    }
    
    
    huemulBigDataGov.close
  }
  
  /**
    masterizacion de archivo [CAMBIAR] <br>
    param_ano: año de los datos  <br>
    param_mes: mes de los datos  <br>
   */
  def process_master(huemulBigDataGov: huemul_BigDataGovernance, ControlParent: huemul_Control, param_ano: Integer, param_mes: Integer): huemul_Control = {
    val Control = new huemul_Control(huemulBigDataGov, ControlParent,  huemulType_Frequency.MONTHLY)    
    
    try {             
      /*************** AGREGAR PARAMETROS A CONTROL **********************/
      Control.AddParamYear("param_ano", param_ano)
      Control.AddParamMonth("param_mes", param_mes)
      
      
      /*************** ABRE RAW DESDE DATALAKE **********************/
      Control.NewStep("Abre DataLake")  
      val DF_RAW =  new raw_institucion_mes(huemulBigDataGov, Control)
      if (!DF_RAW.open("DF_RAW", Control, param_ano, param_mes, 1, 0, 0, 0))       
        Control.RaiseError(s"error encontrado, abortar: ${DF_RAW.Error.ControlError_Message}")
      
      
      /*********************************************************/
      /*************** LOGICAS DE NEGOCIO **********************/
      /*********************************************************/
      //instancia de clase tbl_yourapplication_entidad_mes 
      val huemulTable = new tbl_poc_cmf_activosbanco_mes(huemulBigDataGov, Control)
      
  
   

      val tbl_poc_cmf_product_mes = new tbl_poc_cmf_product_mes(huemulBigDataGov, Control)
      val tbl_poc_cmf_product_mes_data = huemulBigDataGov.DF_ExecuteQuery("df_ins",s"""select TBL_POC_CMF_PRODUCT_MES.PRODUCTO_ID , TBL_POC_CMF_PRODUCT_MES.PRODUCT_DESC from ${tbl_poc_cmf_product_mes.getTable()}​​""").collect()
      

      val tbl_poc_cmf_institucion_mes= new tbl_poc_cmf_institucion_mes(huemulBigDataGov, Control)
      val tbl_poc_cmf_institucion_mes_data = huemulBigDataGov.DF_ExecuteQuery("df_ins",s"""select TBL_POC_CMF_INSTITUCION_MES.INSTITUCION_ID, TBL_POC_CMF_INSTITUCION_MES.INSTITUCION_DESC from ${​​tbl_instituciones.getTable()}​​""").collect()

      val tbl_poc_cmf_activosbanco2_mes = new tbl_poc_cmf_activosbanco2_mes(huemulBigDataGov, Control)
      val tbl_poc_cmf_activosbanco2_mes_data = huemulBigDataGov.DF_ExecuteQuery("df_ins",s"""select * from ${​​tbl_poc_cmf_activosbanco2_mes.getTable()}​​""").collect()
      
      Control.NewStep("Generar Logica de Negocio")
      huemulTable.DF_from_SQL("FinalRAW"
                          , s""" SELECT p.TBL_POC_CMF_PRODUCT_MES.PRODUCTO_ID as producto_id, i.[TBL_POC_CMF_INSTITUCION_MES.INSTITUCION_ID] as institucion_id,
                                ab2.TBL_POC_CMF_ACTIVOSBANCO2_MES.MONTO as monto,ab2.TBL_POC_CMF_ACTIVOSBANCO2_MES.PERIODO_MES as periodo_mes
                                from tbl_poc_cmf_activosbanco2_mes_data as ab2
                                INNER JOIN tbl_comun_producto__mes_data as p ON ab2.TBL_POC_CMF_ACTIVOSBANCO2_MES.PRODUCTO=p.TBL_POC_CMF_PRODUCT_MES.PRODUCT_DESC
                                INNER JOIN tbl_instituciones_mes_data as i ON ab2.TBL_POC_CMF_ACTIVOSBANCO2_MES.INSTITUCIONES = i.TBL_POC_CMF_INSTITUCION_MES.INSTITUCION_DESC;
                              """)
      
      DF_RAW.DataFramehuemul.DataFrame.unpersist()
      
      //comentar este codigo cuando ya no sea necesario generar estadisticas de las columnas.
      Control.NewStep("QUITAR!!! Generar Estadisticas de las columnas SOLO PARA PRIMERA EJECUCION")
      huemulTable.DataFramehuemul.DQ_StatsAllCols(Control, huemulTable)        
      
      Control.NewStep("Asocia columnas de la tabla con nombres de campos de SQL")
      
      huemulTable.periodo_mes.setMapping("periodo_mes")
      huemulTable.institucion_id.setMapping("institucion_id")
      huemulTable.producto_id.setMapping("producto_id")
      huemulTable.monto.setMapping("monto")
   
      

      // huemulTable.setApplyDistinct(false) //deshabilitar si DF tiene datos únicos, por default está habilitado      
      
      Control.NewStep("Ejecuta Proceso")    
      if (!huemulTable.executeFull("FinalSaved"))
        Control.RaiseError(s"User: Error al intentar masterizar los datos (${huemulTable.Error_Code}): ${huemulTable.Error_Text}")
      
      Control.FinishProcessOK
    } catch {
      case e: Exception =>
        Control.Control_Error.GetError(e, this.getClass.getName, null)
        Control.FinishProcessError()
    }
    
    Control
  }
  
}

