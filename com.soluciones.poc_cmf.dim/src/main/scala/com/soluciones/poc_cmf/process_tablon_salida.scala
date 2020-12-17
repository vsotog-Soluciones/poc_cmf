package com.soluciones.poc_cmf_dim



import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import java.util.Calendar
import com.soluciones.tables.master._
import com.soluciones.tables.dim._
import com.soluciones.settings._

//import com.huemulsolutions.bigdata.tables._
//import com.huemulsolutions.bigdata.dataquality._


object process_tablon_salida {
  
  /**
   * Este codigo se ejecuta cuando se llama el JAR desde spark2-submit. el codigo esta preparado para hacer reprocesamiento masivo.
  */
  def main(args : Array[String]) {
    //Creacion API
    val huemulBigDataGov  = new huemul_BigDataGovernance(s"Generacion tablon DIM tabla tbl_poc_cmf_salida - ${this.getClass.getSimpleName}", args, globalSettings.Global)
    
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
      /*
      Control.NewStep("Abre DataLake")  
      val DF_RAW =  new tbl_poc_cmf_activosbanco1_mes(huemulBigDataGov, Control)
      if (!DF_RAW.open("DF_RAW", Control, param_ano, param_mes, 1, 0, 0, 0))       
        Control.RaiseError(s"error encontrado, abortar: ${DF_RAW.Error.ControlError_Message}")
      */
      
      /*********************************************************/
      /*************** LOGICAS DE NEGOCIO **********************/
      /*********************************************************/
      //instancia de clase tbl_yourapplication_entidad_mes 
      

      var tablamaster_activo = new tbl_poc_cmf_activosbanco(huemulBigDataGov, Control) //activos
      var tablamaster_producto = new tbl_poc_cmf_product(huemulBigDataGov, Control) //producto
      var tablamaster_institucion = new tbl_poc_cmf_institucion(huemulBigDataGov, Control) //instituciones

      //var tabladim_sucursal = new tbl_dim_sucursal(huemulBigDataGov, Control) //duda
      //var tabladim_modotasa = new tbl_dim_modotasa(huemulBigDataGov, Control)
      //var tabladim_tipocredito = new tbl_dim_tipocredito(huemulBigDataGov, Control)

      val periodo_mes = huemulBigDataGov.ReplaceWithParams("{{YYYY}}-{{MM}}-{{DD}}", param_ano, param_mes, 1, 0, 0, 0, null)
      val periodo_mesAntFecha = huemulBigDataGov.setDate(periodo_mes)
      periodo_mesAntFecha.add(Calendar.MONTH, -1)
      val periodo_mesAnt = huemulBigDataGov.dateFormat.format(periodo_mesAntFecha.getTime)
      val mes = huemulBigDataGov.ReplaceWithParams("{{YYYY}}{{MM}}", param_ano, param_mes, 0, 0, 0, null)

      
      Control.NewStep("Generar Logica de Negocio")
      val huemulTable = new tbl_dim_poc_cmf_salida_mes(huemulBigDataGov, Control)
      huemulTable.DF_from_SQL("tablon"
                          , s"""
                                SELECT  $mes AS periodo_mes
                                    ,inst.institucion_desc as institucion_desc
                                    ,prod.prod_Path as Producto
                                    ,SUM( CASE WHEN periodo_mes = '$periodo_mes' THEN Monto ELSE 0 END) as Monto
                                    ,SUM( CASE WHEN periodo_mes = '$periodo_mesAnt' THEN Monto ELSE 0 END)  as Monto_ant
                               FROM ${tablamaster_activo.getTable()}  tbl
                               INNER JOIN ${tablamaster_producto.getTable()}  prod
                               ON (tbl.producto_id = prod.producto_id)
                               INNER JOIN ${tablamaster_institucion.getTable()}  inst
                               ON (tbl.institucion_id = inst.institucion_id)

                               WHERE  periodo_mes in ('$periodo_mes','$periodo_mesAnt')
                               AND prod.producto_id IN (2011221, 2011231 , 2011321, 2011331, 2012211, 2012432, 2012433, 2012434)
                               GROUP BY inst.institucion_desc , prod.prod_Path
                               """)
      
      //tablamaster_activo.DataFramehuemul.DataFrame.unpersist()
      
      //comentar este codigo cuando ya no sea necesario generar estadisticas de las columnas.
      //Control.NewStep("QUITAR!!! Generar Estadisticas de las columnas SOLO PARA PRIMERA EJECUCION")
      //huemulTable.DataFramehuemul.DQ_StatsAllCols(Control, huemulTable)        
      
      Control.NewStep("Asocia columnas de la tabla con nombres de campos de SQL")
      
      huemulTable.periodo_mes.setMapping("periodo_mes")
      huemulTable.institucion_nom.setMapping("institucion_desc")
      huemulTable.producto_nom.setMapping("Producto")
      huemulTable.activo_mon.setMapping("Monto")
      huemulTable.activo_mon_mes_ant.setMapping("Monto_ant")
      

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

