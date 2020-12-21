## Conversión xlsx a csv en Pyhton 
## Resumen

_Estas instrucciones permitirán convertir los archivos de la carpeta data de xlsx a csv mediante instrucciones escritas en Python 2.7._

### Pre-requisitos 📋 

```
xlrd 1.2.0
pandas 0.24.2
unicodecsv 0.14.1
```

### Instalación 🔧

_Se debe instalar la versión  1.2.0 de xlrd para el correcto funcionamiento del código_
```
pip install xlrd==1.2.0
```
_las demas librerias_

```
pip install pandas
pip install unicodecsv
```
## Ejecución ⚙️

El código Python leerá los archivos xlsx que se encuentran en la carpeta “data”, los cuales corresponde a 13 archivos Excel, con información de las cuotas de mercado del sistema bancario chileno.

Una vez leídos se considerarán solo dos pestañas, Activos Bancos 1 y Activos Banco 2, las cuales se convertirán en un csv cada una para cada año y mes. Obteniendo como resultados 26 csv los cuales serán guardados en la capeta “csv_utf_python” en un formato UTF-8. 
Las rutas de entrada con los 13 archivos se definen en:

* location_excel = 'data/'

y la ruta de Salida con los csv resultantes en:

* out_csv = 'csv_utf_python/'
