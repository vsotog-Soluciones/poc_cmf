package com.soluciones.poc_cmf.datalake
        
//import com.soluciones.poc_cmf.globalSettings._
import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.datalake._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import com.soluciones.settings.globalSettings._

//ESTE CODIGO FUE GENERADO A PARTIR DEL TEMPLATE DEL SITIO WEB


/**
 * Clase que permite abrir un archivo de texto, devuelve un objeto huemul_dataLake con un DataFrame de los datos
 * ejemplo de nombre: raw_institucion_mes
 */
class raw_entidadPDF_mes(huemulBigDataGov: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_DataLake(huemulBigDataGov, Control) with Serializable  {
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
   //para este ejemplo, descargar datos desde "http://www.ins.gob.pe/insvirtual/images/otrpubs/pdf/Tabla%20de%20Alimentos.pdf"
     .setFileName("[[name.TXT]]") //
   //especifica el tipo de archivo a leer
     .setFileType(huemulType_FileType.PDF_FILE)
   //expecifica el nombre del contacto del archivo en TI
     .setContactName("[[nombre de contacto del origen]]")

   //Indica como se lee el archivo
     .setColumnDelimiterType(huemulType_Separator.CHARACTER)  //POSITION;CHARACTER
   //separador de columnas
      .setColumnDelimiter("\t")    //SET FOR CARACTER
   //forma rápida de configuración de columnas del archivo
   //CurrentSetting.DataSchemaConf.setHeaderColumnsString("institucion_id;institucion_nombre")
   //Forma detallada
      .addColumn("Codigo", "Codigo", StringType, "Codigo de la tabla")
     .addColumn("Grupo", "Grupo", StringType, "Grupo")
    
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
   * Retorna: true si  está OK, false si tuvo algún problema <br>
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
      
      control.NewStep("Obtiene Posicion inicial y final dentro de PDF")
      //Según el PDF, se deben buscar caracteres o título para buscar el inicio y fin de los datos a rescatar
      //*******************************************************
      //obtiene posicion inicial
      //*******************************************************
      val reg_filaInicial = this.DataRDD_extended.filter { f => f._4.startsWith("Códigos y grupos de alimentos")}.collect()
      if (reg_filaInicial.length == 0)
        RaiseError_RAW("Texto inicial no encontrado", 1)  
      val filaInicial = reg_filaInicial(0)._1
      
      //*******************************************************
      //obtiene posicion final
      //*******************************************************
      val reg_filaFinal = this.DataRDD_extended.filter { f => f._4.startsWith("Componentes: definición y expresión de nutrientes")}.collect()
      if (reg_filaFinal.length == 0)
        RaiseError_RAW("Texto final no encontrado", 2)
      val filaFinal = reg_filaFinal(0)._1
      
      control.NewStep("Declaración de expresiones regulares y exclusiones varias")
      //*******************************************************
      //genera expresiones regulares
      //exlusiones (por ejemplo textos de pies de págian, etc
      //*******************************************************
      val regexCodigo = "[A-Z]".r
      val filtroCirculares = "Instituto Nacional de Salud".toUpperCase()
      
      control.NewStep("Aplicando Filtro sobre PDF")
      //el objeto DataRDD_extended contiene lo siguiente cuatro columnas:
      //    x._1 -> fila leia desde PDF, esto es un correlativo del PDF
      //    x._2 -> entrega el largo de la fila, equivalente a realizar x._4.length()
      //    x._3 -> entrega el largo de la fila con trim, equivalente a x._4.trim().length()
      //    x._4 -> texto obtenido desde PDF
      
      //a partir del DataRDD_extended, se genera un nuevo RDD llamado rowRDD_Base, el cual conendrá 6 columnas 
      //que son generadas en la sentencia .map
      //la fila específica que se quiere obtener tiene la forma "CODIGO DESCRIPCION DE LO QUE TIENE EL CODIGO"
      //ejemplo:                                                "A descripcion de A"
      //por tanto lo que hacemos es un split(" "), eso nos entrega un arreglo con dos posiciones
      //en la primera está el código, en la segunda está el resto del texto, equivalente a la descripción.
      val rowRDD_Base = this.DataRDD_extended
            .filter { x => x._1 >= filaInicial && x._1 < filaFinal}    //caracteres iniciales
            .filter { x => x._3 > 0  }                                 //largo de fila con trim > 0
            .filter { x => !(x._4.toUpperCase() == filtroCirculares) } //filtro de textos que no deben ser considerados
            .map ( x=> (x._1 //posicion
                      , x._2 //largo
                      , x._3 //largo con trim
                      , x._4 //texto
                      , x._4.split(" ",2) //_5 split
                      , regexCodigo.findFirstMatchIn(x._4.split(" ",2)(0)).mkString.trim().length() == 1 && x._4.split(" ",2)(0).length() == 1  //_6 marcador de existencia de codigo
                      )).collect()
                      
      //******************************************
      //IMPRESION DE RESULTADOS POR CONSOLA
      //******************************************
      println(s"********************* RESULTADO rowRDD_Base, cantidad filas: ${rowRDD_Base.length}")
      rowRDD_Base.foreach(println)
      
      control.NewStep("Transformando datos a RDD consolidado con datos finales filtrados")     
      //*******************************************************
      //Genera resultado final de la tabla
      //*******************************************************
      val rowRDD_Consolidado = rowRDD_Base.filter{x => x._6} .map(x => (x._4 //código y texto original
                                                ,x._5(0)
                                                ,x._5(1).trim()
                                               )
                                         )
                                         
      //*******************************************************
      //muestra ejemplo de los datos del arreglo CONSOLIDADO
      //*******************************************************
      println("")
      println("")
      println("********************************** RESULTADO RDD CONSOLIDADO")
      rowRDD_Consolidado.foreach { x => println(x) }
      println("")
      println("")
      
      //import huemulBigDataGov.spark.implicits._
      control.NewStep("RDD Final")     
      val rowRDD = rowRDD_Consolidado
            .map{ x=>
                  val DataArray_Dest : Array[Any] = new Array[Any](2)
                  DataArray_Dest(0) = x._2
                  DataArray_Dest(1) = x._3
                  Row.fromSeq(DataArray_Dest )
                }
                  
      println("")
      println("")
      println("********************************** RESULTADO RDD FINAL")
      rowRDD.foreach { x => println(x) }      
      println("")
      println("")
      
            
      control.NewStep("Transformando datos a dataframe")      
      //Crea DataFrame en Data.DataDF
      this.DF_from_RAW(huemulBigDataGov.spark.sparkContext.parallelize(rowRDD), Alias)
        
      //****VALIDACION DQ*****
      //**********************
      control.NewStep("Valida que cantidad de registros esté entre 1 y 100")    
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
object raw_entidadPDF_mes_test {
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
    val DF_RAW =  new raw_entidadPDF_mes(huemulBigDataGov, Control)
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
