package com.soluciones.poc_cmf.datalake
        
//import com.soluciones.poc_cmf.globalSettings._
import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.datalake._
import org.apache.spark.sql.types._
import com.soluciones.settings.globalSettings._

//ESTE CODIGO FUE GENERADO A PARTIR DEL TEMPLATE DEL SITIO WEB


/**
 * Clase que permite abrir un archivo de texto, devuelve un objeto huemul_dataLake con un DataFrame de los datos
 * ejemplo de nombre: raw_institucion_mes
 */
class raw_activosbanco2_mes(huemulBigDataGov: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_DataLake(huemulBigDataGov, Control) with Serializable  {
   this.Description = "PRINCIPALES ACTIVOS CONSOLIDADOS POR INSTITUCIONES"
   this.GroupName = "poc_cmf"
   this.setFrequency(huemulType_Frequency.MONTHLY)
   
   //Crea variable para configuración de lectura del archivo
   val CurrentSetting: huemul_DataLakeSetting = new huemul_DataLakeSetting(huemulBigDataGov)
   //setea la fecha de vigencia de esta configuración
      .setStartDate(2010,1,1,0,0,0)
      .setEndDate(2050,12,12,0,0,0)
   //Configuración de rutas globales
      .setGlobalPath(huemulBigDataGov.GlobalSettings.RAW_SmallFiles_Path)
   //Configura ruta local, se pueden usar comodines
      .setLocalPath("poc_cmf/")
   //configura el nombre del archivo (se pueden usar comodines)
      .setFileName("ab2{{YYYY}}{{MM}}_cmf.csv")
   //especifica el tipo de archivo a leer
     .setFileType(huemulType_FileType.TEXT_FILE)
   //expecifica el nombre del contacto del archivo en TI
     .setContactName("CMF")
   //Indica como se lee el archivo
     .setColumnDelimiterType(huemulType_Separator.CHARACTER)  //POSITION;CHARACTER
   //separador de columnas
     .setColumnDelimiter(";")    //SET FOR CARACTER
   //forma rápida de configuración de columnas del archivo
   //CurrentSetting.DataSchemaConf.setHeaderColumnsString("institucion_id;institucion_nombre")
   //Forma detallada
     .addColumn("Instituciones", "Instituciones", StringType, "Instituciones")
     .addColumn("activos_adeudado_por_bancos_neto_de_provisiones_total", "activos_adeudado_por_bancos_neto_de_provisiones_total", DecimalType(10,2), "activos_adeudado_por_bancos_neto_de_provisiones_total")
     .addColumn("activos_adeudado_por_bancos_neto_de_provisiones_bancos_del_pais_total_1", "activos_adeudado_por_bancos_neto_de_provisiones_bancos_del_pais_total_1", DecimalType(10,2), "activos_adeudado_por_bancos_neto_de_provisiones_bancos_del_pais_total_1")
     
     
     .addColumn("activos_adeudado_por_bancos_neto_de_provisiones_bancos_del_pais_prestamos_interbancarios", "activos_adeudado_por_bancos_neto_de_provisiones_bancos_del_pais_prestamos_interbancarios", DecimalType(10,2), "activos_adeudado_por_bancos_neto_de_provisiones_bancos_del_pais_prestamos_interbancarios")

     .addColumn("activos_adeudado_por_bancos_neto_de_provisiones_bancos_del_pais_creditos_de_comercio_exterior", "activos_adeudado_por_bancos_neto_de_provisiones_bancos_del_pais_creditos_de_comercio_exterior", DecimalType(10,2), "activos_adeudado_por_bancos_neto_de_provisiones_bancos_del_pais_creditos_de_comercio_exterior")

     .addColumn("activos_adeudado_por_bancos_neto_de_provisiones_bancos_del_pais_provisiones_para_creditos_con_bancos_del_pais", "activos_adeudado_por_bancos_neto_de_provisiones_bancos_del_pais_provisiones_para_creditos_con_bancos_del_pais", DecimalType(10,2), "activos_adeudado_por_bancos_neto_de_provisiones_bancos_del_pais_provisiones_para_creditos_con_bancos_del_pais")

     .addColumn("activos_adeudado_por_bancos_neto_de_provisiones_bancos_del_exterior_total_1", "activos_adeudado_por_bancos_neto_de_provisiones_bancos_del_exterior_total_1", DecimalType(10,2), "activos_adeudado_por_bancos_neto_de_provisiones_bancos_del_exterior_total_1")

     .addColumn("activos_adeudado_por_bancos_neto_de_provisiones_bancos_del_exterior_prestamos_interbancarios", "activos_adeudado_por_bancos_neto_de_provisiones_bancos_del_exterior_prestamos_interbancarios", DecimalType(10,2), "activos_adeudado_por_bancos_neto_de_provisiones_bancos_del_exterior_prestamos_interbancarios")

     .addColumn("activos_adeudado_por_bancos_neto_de_provisiones_bancos_del_exterior_creditos_de_comercio_exterior", "activos_adeudado_por_bancos_neto_de_provisiones_bancos_del_exterior_creditos_de_comercio_exterior", DecimalType(10,2), "activos_adeudado_por_bancos_neto_de_provisiones_bancos_del_exterior_creditos_de_comercio_exterior")

     .addColumn("activos_adeudado_por_bancos_neto_de_provisiones_bancos_del_exterior_provisiones_para_creditos_con_bancos_del_exterior", "activos_adeudado_por_bancos_neto_de_provisiones_bancos_del_exterior_provisiones_para_creditos_con_bancos_del_exterior", DecimalType(10,2), "activos_adeudado_por_bancos_neto_de_provisiones_bancos_del_exterior_provisiones_para_creditos_con_bancos_del_exterior")

     .addColumn("activos_adeudado_por_bancos_neto_de_provisiones_banco_central_de_chile", "activos_adeudado_por_bancos_neto_de_provisiones_banco_central_de_chile", DecimalType(10,2), "activos_adeudado_por_bancos_neto_de_provisiones_banco_central_de_chile")

     .addColumn("activos_creditos_y_cuentas_por_cobrar_a_clientes__total_1", "activos_creditos_y_cuentas_por_cobrar_a_clientes__total_1", DecimalType(10,2), "activos_creditos_y_cuentas_por_cobrar_a_clientes__total_1")

     .addColumn("activos_creditos_y_cuentas_por_cobrar_a_clientes__provisiones_constituidas_de_cred_y_ctas_por_cob_a_clientes", "activos_creditos_y_cuentas_por_cobrar_a_clientes__provisiones_constituidas_de_cred_y_ctas_por_cob_a_clientes", DecimalType(10,2), "activos_creditos_y_cuentas_por_cobrar_a_clientes__provisiones_constituidas_de_cred_y_ctas_por_cob_a_clientes")

     .addColumn("activos_creditos_y_cuentas_por_cobrar_a_clientes__colocaciones_comerciales_empresas_1_colocaciones", "activos_creditos_y_cuentas_por_cobrar_a_clientes__colocaciones_comerciales_empresas_1_colocaciones", DecimalType(10,2), "activos_creditos_y_cuentas_por_cobrar_a_clientes__colocaciones_comerciales_empresas_1_colocaciones") 

     .addColumn("activos_creditos_y_cuentas_por_cobrar_a_clientes__colocaciones_comerciales_empresas_1_provisiones", "activos_creditos_y_cuentas_por_cobrar_a_clientes__colocaciones_comerciales_empresas_1_provisiones", DecimalType(10,2), "activos_creditos_y_cuentas_por_cobrar_a_clientes__colocaciones_comerciales_empresas_1_provisiones") 

     .addColumn("activos_creditos_y_cuentas_por_cobrar_a_clientes__personas_total", "activos_creditos_y_cuentas_por_cobrar_a_clientes__personas_total", DecimalType(10,2), "activos_creditos_y_cuentas_por_cobrar_a_clientes__personas_total")

     .addColumn("activos_creditos_y_cuentas_por_cobrar_a_clientes__personas_provisiones", "activos_creditos_y_cuentas_por_cobrar_a_clientes__personas_provisiones", DecimalType(10,2), "activos_creditos_y_cuentas_por_cobrar_a_clientes__personas_provisiones")

     .addColumn("activos_creditos_y_cuentas_por_cobrar_a_clientes__personas_consumo_1_total", "activos_creditos_y_cuentas_por_cobrar_a_clientes__personas_consumo_1_total", DecimalType(10,2), "activos_creditos_y_cuentas_por_cobrar_a_clientes__personas_consumo_1_total")

     .addColumn("activos_creditos_y_cuentas_por_cobrar_a_clientes__personas_consumo_1_en_cuotas", "activos_creditos_y_cuentas_por_cobrar_a_clientes__personas_consumo_1_en_cuotas", DecimalType(10,2), "activos_creditos_y_cuentas_por_cobrar_a_clientes__personas_consumo_1_en_cuotas")

     .addColumn("activos_creditos_y_cuentas_por_cobrar_a_clientes__personas_consumo_1_tarjetas_de_credito", "activos_creditos_y_cuentas_por_cobrar_a_clientes__personas_consumo_1_tarjetas_de_credito", DecimalType(10,2), "activos_creditos_y_cuentas_por_cobrar_a_clientes__personas_consumo_1_tarjetas_de_credito")

     .addColumn("activos_creditos_y_cuentas_por_cobrar_a_clientes__personas_consumo_1_otros", "activos_creditos_y_cuentas_por_cobrar_a_clientes__personas_consumo_1_otros", DecimalType(10,2), "activos_creditos_y_cuentas_por_cobrar_a_clientes__personas_consumo_1_otros")

     .addColumn("activos_creditos_y_cuentas_por_cobrar_a_clientes__personas_consumo_1_provisiones", "activos_creditos_y_cuentas_por_cobrar_a_clientes__personas_consumo_1_provisiones", DecimalType(10,2), "activos_creditos_y_cuentas_por_cobrar_a_clientes__personas_consumo_1_provisiones")

     .addColumn("activos_creditos_y_cuentas_por_cobrar_a_clientes__personas_vivienda_1_colocaciones", "activos_creditos_y_cuentas_por_cobrar_a_clientes__personas_vivienda_1_colocaciones", DecimalType(10,2), "activos_creditos_y_cuentas_por_cobrar_a_clientes__personas_vivienda_1_colocaciones")

     .addColumn("activos_creditos_y_cuentas_por_cobrar_a_clientes__personas_vivienda_1_provisiones", "activos_creditos_y_cuentas_por_cobrar_a_clientes__personas_vivienda_1_provisiones", DecimalType(10,2), "activos_creditos_y_cuentas_por_cobrar_a_clientes__personas_vivienda_1_provisiones")

     .addColumn("colocaciones", "colocaciones", DecimalType(10,2), "colocaciones")

     
   //Seteo de lectura de información de Log (en caso de tener)
     .setHeaderColumnDelimiterType(huemulType_Separator.CHARACTER)  //POSITION;CHARACTER;NONE
     .setHeaderColumnDelimiter(";")
     .setHeaderColumnsString("VACIO")
     .setLogNumRowsColumnName(null)

   this.SettingByDate.append(CurrentSetting)
  
    /***
   * open(ano: Int, mes: Int) <br>
   * método que retorna una estructura con un DF de detalle, y registros de control <br>
   * ano: año de los archivos recibidos <br>
   * mes: mes de los archivos recibidos <br>
   * dia: dia de los archivos recibidos <br>
   * Retorna: true si todo está OK, false si tuvo algún problema <br>
  */
  def open(Alias: String, ControlParent: huemul_Control, ano: Integer, mes: Integer, dia: Integer, hora: Integer, min: Integer, seg: Integer): Boolean = {
    //Crea registro de control de procesos
     val control = new huemul_Control(huemulBigDataGov, ControlParent, huemulType_Frequency.ANY_MOMENT)
    //Guarda los parámetros importantes en el control de procesos
    control.AddParamYear("Ano", ano)
    control.AddParamMonth("Mes", mes)
       
    try { 
      //NewStep va registrando los pasos de este proceso, también sirve como documentación del mismo.
      control.NewStep("Abre archivo RDD y devuelve esquemas para transformar a DF")
      if (!this.OpenFile(ano, mes, dia, hora, min, seg, null)){
        //Control también entrega mecanismos de envío de excepciones
        control.RaiseError(s"Error al abrir archivo: ${this.Error.ControlError_Message}")
      }
   
      control.NewStep("Aplicando Filtro")
      //Si el archivo no tiene cabecera, comentar la línea de .filter
      val rowRDD = this.DataRDD
            //filtro para considerar solo las filas que los tres primeros caracteres son numéricos
            //.filter { x => x.length()>=4 && huemulBigDataGov.isAllDigits(x.substring(0, 3) )  }
            //filtro para dejar fuera la primera fila
            .filter { x => x != this.Log.DataFirstRow  }
            .map { x => this.ConvertSchema(x) }
            
      control.NewStep("Transformando datos a dataframe")      
      //Crea DataFrame en Data.DataDF
      this.DF_from_RAW(rowRDD, Alias)
        
      //****VALIDACION DQ*****
      //**********************
      control.NewStep("Valida que cantidad de registros esté entre 0 y 100")    
      //validacion cantidad de filas
      val validanumfilas = this.DataFramehuemul.DQ_NumRowsInterval(this, 0, 100)      
      if (validanumfilas.isError) control.RaiseError(s"user: Numero de Filas fuera del rango. ${validanumfilas.Description}")
                        
      control.FinishProcessOK                      
    } catch {
      case e: Exception =>
        control.Control_Error.GetError(e, this.getClass.getName, null)
        control.FinishProcessError()   

    }         

    control.Control_Error.IsOK()
  }
}


/**
 * Este objeto se utiliza solo para probar la lectura del archivo RAW
 * La clase que está definida más abajo se utiliza para la lectura.
 */
object raw_activosbanco2_mes_test {
   /**
   * El proceso main es invocado cuando se ejecuta este código
   * Permite probar la configuración del archivo RAW
   */
  
  def main(args : Array[String]) {
    
    //Creación API
    val huemulBigDataGov  = new huemul_BigDataGovernance(s"Testing DataLake - ${this.getClass.getSimpleName}", args, Global)
    //Creación del objeto control, por default no permite ejecuciones en paralelo del mismo objeto (corre en modo SINGLETON)
    val Control = new huemul_Control(huemulBigDataGov, null, huemulType_Frequency.MONTHLY )
    
    /*************** PARAMETROS **********************/
    val param_ano = huemulBigDataGov.arguments.GetValue("ano", null, "Debe especificar el parámetro año, ej: ano=2017").toInt
    val param_mes = huemulBigDataGov.arguments.GetValue("mes", null, "Debe especificar el parámetro mes, ej: mes=12").toInt
    
    //Inicializa clase RAW  
    val DF_RAW =  new raw_activosbanco2_mes(huemulBigDataGov, Control)
    if (!DF_RAW.open("DF_RAW", null, param_ano, param_mes, 0, 0, 0, 0)) {
      println("************************************************************")
      println("**********  E  R R O R   E N   P R O C E S O   *************")
      println("************************************************************")
    } else
      DF_RAW.DataFramehuemul.DataFrame.show()
      
    Control.FinishProcessOK
    huemulBigDataGov.close()
   
  }  
}
