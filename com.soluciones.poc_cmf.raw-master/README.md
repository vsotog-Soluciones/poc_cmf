# POC GDD  DATOS CMF 


## Instrucciones
* Abrir el archivo pom.xml y cambiar los siguientes elementos:
    - Group Id
    - Artifact Id
    - Version
    - Project Name
    - Descripcion
    
* Ajustar los packages en función del Group Id y Artifact Id que tiene tu proyecto.
* Se recomienda eliminar el fichero "globalSettings.scala" y utilizar un proyecto por separado con esta configuración.

## Lógica de ejecución 
 
El código genera las entidades de negocio, generando un modelo en tercera 
forma normal, el cual pueda ser explotado por usuarios finales como estrategia self service. 

Para lograr lo anterior, se debe realizar la ejecucion de un process(process_raw_master), el cual se ejecuta con el siguiente comando : 

    sudo -su hdfs spark-submit --master local --jars huemul-bigdatagovernance-2.6.2.jar,huemul-sql-decode-1.0.jar,poc_settings-2.6.2.jar,postgresql-9.4.1212.jar --class com.soluciones.poc_cmf.process_raw_master poc_cmf-2.6.2.jar environment=production,ano=2019,mes=09

El destino de esta es una capa en HDFS, llamada Master. Este último es donde deben reposar las tablas
(tbl_poc_cmf_institucion,tbl_poc_cmf_product_n1,tbl_poc_cmf_product_n2,tbl_poc_cmf_product_n3,tbl_poc_cmf_product_n4
tbl_poc_cmf_product_n5,tbl_poc_cmf_product) del modelo de tercera forma normal con formato parquet, tratamiento que debe ser generado con Apache 
Spark.En el mismo package com.soluciones.poc_cmf.raw-master, genera las entidades de negocio, generando un modelo de los activos banco 1 y activos banco 2
Para lograr lo anterior ,  se debe realizar la ejecucion de un process(process_raw_transaction), el cual  con el siguiente comando  : 

Se realiza un ciclo de reproceso masivo, agregando parámetros de control , especificando año , mes y numeros de meses a procesar

 --Periodo Inicio - Periodo Fin 
     201909           201912
     
     sudo -su hdfs spark-submit --master local --jars huemul-bigdatagovernance-2.6.2.jar,huemul-sql-decode-1.0.jar,poc_settings-2.6.2.jar,postgresql-9.4.1212.jar --class com.soluciones.poc_cmf.process_raw_transaction poc_cmf-2.6.2.jar environment=production,ano=2019,mes=09,num_meses=4
     
  --Periodo Inicio - Periodo Fin 
     202001           202009    

    sudo -su hdfs spark-submit --master local --jars huemul-bigdatagovernance-2.6.2.jar,huemul-sql-decode-1.0.jar,poc_settings-2.6.2.jar,postgresql-9.4.1212.jar --class com.soluciones.poc_cmf.process_raw_transaction poc_cmf-2.6.2.jar environment=production,ano=2020,mes=01,num_meses=9

