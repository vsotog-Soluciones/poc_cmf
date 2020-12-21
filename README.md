## Proyecto de POC para el banco de chile con datos de CMF
## Resumen

El objetivo de este documento es dar a conocer el alcance de la prueba de concepto (POC) que se realizará en Banco de Chile, con el departamento Big Data de la División de Operaciones y Tecnología, todo asociado a información pública. La POC debe desenvolverse en los ambientes de cada proveedor.

## CONCEPTOS CLAVES 


![](com.soluciones.poc_settings-master/images.png)

- CMF: Comisión para el mercado financiero 
- Activo: Deudas de los clientes 
- Pasivo: Son las obligaciones que tienen los bancos. Ejemplo: cuenta corriente 
- Cuota de mercado: es la proporción de mercado que consume los productos o servicios de una empresa determinada 

## REQUISITOS 




- Python 2.7 y Scala 
- Apache Maven 3.6.3 (cecedd343002696d0abb50b32b541b8a6ba2883f)
- Java version "1.8.0_241"
- Java(TM) SE Runtime Environment (build 1.8.0_241-b07)
- Java HotSpot(TM) 64-Bit Server VM (build 25.241-b07, mixed mode)

## Lógica de ejecución 
 
El código genera las entidades de negocio, generando un modelo en tercera 
forma normal, el cual pueda ser explotado por usuarios finales como estrategia self service. 

Para lograr lo anterior, se debe realizar la ejecucion de un process(process_raw_master), el cual se ejecuta con el siguiente comando : 

    sudo -su hdfs spark-submit --master local --jars huemul-bigdatagovernance-2.6.2.jar,huemul-sql-decode-1.0.jar,poc_settings-2.6.2.jar,postgresql-9.4.1212.jar --class com.soluciones.poc_cmf.process_raw_master poc_cmf-2.6.2.jar environment=production,ano=2019,mes=09

el destino de esta es una capa en HDFS, llamada Master. Este último es donde deben reposar las tablas
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



## Historial de versiones

* 0.0.1
    * first commit
* 0.0.2
    * CAMBIO: Cambios de POM
* 0.0.3
    * CAMBIO: Procesos nuevos
* 0.0.4
    * CAMBIO: Incorporación de institucion y producto
* 0.0.5
    * CAMBIO: Prueba de Procesos
* 0.0.6
    * CAMBIO: Proceso para generar tablon dimensinal                   


## Meta

vsotog-Soluciones - vsotog@soluciones.cl

Distribuido bajo la licencia XYZ. Ver ``LICENSE`` para más información.

[https://github.com/vsotog-Soluciones]
