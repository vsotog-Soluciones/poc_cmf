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
class raw_entidadAVRO_mes(huemulBigDataGov: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_DataLake(huemulBigDataGov, Control) with Serializable  {
   this.Description = "Decripción de la interfaz"
   this.GroupName = "[[yourapplication]]"
   this.setFrequency(huemulType_Frequency.MONTHLY)
   
   //Crea variable para configuración de lectura del archivo
   val CurrentSetting: huemul_DataLakeSetting = new huemul_DataLakeSetting(huemulBigDataGov)
   //setea la fecha de vigencia de esta configuración
     .setStartDate(2010,1,1,0,0,0)
     .setEndDate(2050,12,12,0,0,0)
   //Configuración de rutas globales
     .setGlobalPath(huemulBigDataGov.GlobalSettings.RAW_SmallFiles_Path)
   //Configura ruta local, se pueden usar comodines
     .setLocalPath("[[yourapplication]]/")
   //configura el nombre del archivo (se pueden usar comodines)
     .setFileName("[[name_avro_file.avro]]")
   //especifica el tipo de archivo a leer
     .setFileType(huemulType_FileType.AVRO_FILE)
   //expecifica el nombre del contacto del archivo en TI
     .setContactName("[[nombre de contacto del origen]]")

   //Indica como se lee el archivo
     .setColumnDelimiterType(huemulType_Separator.CHARACTER)  //POSITION;CHARACTER
   //separador de columnas
     .setColumnDelimiter("\t")    //SET FOR CARACTER
   //forma rápida de configuración de columnas del archivo
   //CurrentSetting.DataSchemaConf.setHeaderColumnsString("institucion_id;institucion_nombre")
   //Forma detallada
     .addColumn("ejemplo_producto_id", "pk", IntegerType, "codigo del producto")
     .addColumn("fecha_venta", "fecven", StringType, "fecha de la venta")
     .addColumn("cantidad", "cant", DecimalType(10,2), "Cantidad del producto")
     .addColumn("precio", "prec", DecimalType(10,2), "Precio de la transaccion")
    
   //Seteo de lectura de información de Log (en caso de tener)
      .setHeaderColumnDelimiterType(huemulType_Separator.NONE)  //POSITION;CHARACTER;NONE
      .setLogNumRowsColumnName(null)
      .setHeaderColumnDelimiter(";")    //SET FOR CARACTER
      .setHeaderColumnsString("VACIO")


   this.SettingByDate.append(CurrentSetting)
  
    /***
   * open(ano: Int, mes: Int) <br>
   * método que retorna una estructura con un DF de detalle, y registros de control <br>
   * ano: año de los archivos recibidos <br>
   * mes: mes de los archivos recibidos <br>
   * dia: dia de los archivos recibidos <br>
   * Retorna: true si está OK, false si tuvo algún problema <br>
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
            
      control.NewStep("Transformando datos a dataframe")      
      //Crea DataFrame en Data.DataDF
      this.DF_from_RAW(this.DataRawDF, Alias)
        
      //****VALIDACION DQ*****
      //**********************
      control.NewStep("Valida que cantidad de registros esté entre 10 y 100")    
      //validacion cantidad de filas
      val validanumfilas = this.DataFramehuemul.DQ_NumRowsInterval(this, 10, 100)      
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
object raw_entidadAVRO_mes_test {
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
    val DF_RAW =  new raw_entidadAVRO_mes(huemulBigDataGov, Control)
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
