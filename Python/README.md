## Proyecto de POC para el banco de chile con datos de CMF
## Resumen

El objetivo de este documento es dar a conocer el alcance de la prueba de concepto (POC) que se realizará en Banco de Chile, con el departamento Big Data de la División de Operaciones y Tecnología, todo asociado a información pública. La POC debe desenvolverse en los ambientes de cada proveedor.

## CONCEPTOS CLAVES 


![](Home.png)

i. CMF: Comisión para el mercado financiero ii. Activo: Deudas de los clientes iii. Pasivo: Son las obligaciones que tienen los bancos. Ejemplo: cuenta corriente iv. Cuota de mercado: es la proporción de mercado que consume los productos o servicios de una empresa determinada 




## Instalación

OS X , Windows y Linux:


```sh

Rename .env.example to .env and fill the options.


composer install
npm install
php artisan key:generate
php artisan migrate
php artisan db:seed
gulp
php artisan serve
```



```sh
edit autoexec.bat
```

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
    * CAMBIO: Actualizada la documentación (el módulo de código permanece igual)


## Meta

Jorge Valdes FLores – (@jorgevaldes47) – jorge.valdes.01@alu.ucm.cl

Distribuido bajo la licencia XYZ. Ver ``LICENSE`` para más información.

[https://github.com/JorgeValdes]