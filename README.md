## Proyecto de POC para el banco de chile con datos de CMF
## Resumen

El objetivo de este documento es dar a conocer el alcance de la prueba de concepto (POC) que se realizará en Banco de Chile, con el departamento Big Data de la División de Operaciones y Tecnología, todo asociado a información pública. La POC debe desenvolverse en los ambientes de cada proveedor.

## CONCEPTOS CLAVES 


![](images.png)

i. CMF: Comisión para el mercado financiero 
ii. Activo: Deudas de los clientes 
iii. Pasivo: Son las obligaciones que tienen los bancos. Ejemplo: cuenta corriente 
iv. Cuota de mercado: es la proporción de mercado que consume los productos o servicios de una empresa determinada 



## Requisitos

OS X , Windows y Linux:


```sh

Python 2.7
java version "1.8.0_241"
Java(TM) SE Runtime Environment (build 1.8.0_241-b07)
Java HotSpot(TM) 64-Bit Server VM (build 25.241-b07, mixed mode)
Apache Maven 3.6.3 (cecedd343002696d0abb50b32b541b8a6ba2883f)
HDFS y entornos distribuidos
Shell 
GIT
```



```sh

```


## Lógica de ejecución

OS X , Windows y Linux:
   
    El código reconoce las entidades de negocio, generando un modelo en tercera forma normal,
    el cual pueda ser explotado por usuarios finales

    el comando para nuestra ejecucion, para la capa Master es la llamada de nuestro process_raw_master

```sh

 

    sudo -su hdfs spark-submit --master local --jars huemul-bigdatagovernance-2.6.2.jar,huemul-sql-decode-1.0.jar,poc_settings-2.6.2.jar,postgresql-9.4.1212.jar --class com.soluciones.poc_cmf.process_raw_master poc_cmf-2.6.2.jar environment=production,ano=2019,mes=09.

  



```sh
edit autoexec.bat
```
  El cual a su vez, reprocesa los 7 distintos procesos para generar nuestro modelo con formato parquet
    generado con Apache Spark

    ![](raw_master.png)

## Ejemplo de uso

Algunos casos de ejemplo sobre cómo utilizar tu producto. Algunos bloques de código y capturas de pantalla harán que sea más atractivo.

## Configuración de desarrollo


```sh
Creación y Autenticación de Usuarios

Hash para passwords

Paginación

Subida de Archivos

Seguridad y Protección

Webpack para añadir CSS o Librerías JS

Envio de Emails

Confirmación de Cuentas

Sanitización de Inputs

Con todo esto podrás crear aplicaciones web modernas, pero el curso va más allá, aprenderás otros temas tales como

Integrar Librerías JavaScript con tus aplicaciones Laravel tales como Sweet Alert 2, Dropzone JS, MomentJS y mucho más

Eloquent para relacionar Tablas y crear aplicaciones más robustas y dinamicas

Integrar el framework VueJS en Laravel

Agregar Vue Router y Vuex a Laravel

Crear API's con Laravel que se consumirán con Vue

Agregar Tailwind CSS a tus proyectos

Crear proyectos Full Stack en Laravel

Notificaciones y Middleware


```

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