# censo2022arg 1.0.0

Primera version publica del paquete.

## Funciones principales

* `censo_configurar()` -- configura el directorio de datos del paquete
* `censo_info()` -- muestra el estado actual de la configuracion
* `censo_verificar_engine()` -- verifica y guia la preparacion del motor REDATAM
* `censo_descargar()` -- descarga bases y documentacion desde el INDEC
* `extraer_redatam()` -- extrae microdatos del Censo 2022 provincia por provincia
* `censo_etiquetar()` -- etiqueta variables con los diccionarios oficiales del INDEC
* `censo_leer()` -- lee microdatos extraidos con soporte para filtros y seleccion de columnas
* `extraer_rxdb()` -- extrae microdatos de cualquier base RedatamX generica

## Caracteristicas tecnicas

* Extraccion en bloques con subprocesos independientes para gestion eficiente de RAM
* Reconstruccion de identificadores jerarquicos de vivienda y hogar
* Verificacion automatica de integridad contra totales oficiales del INDEC
* Soporte para formato Parquet (default), CSV, SPSS y SAS
* Pipeline VP+PO: combina radio censal de Base VP con variables adicionales de Base PO
* Proceso retomable: si se interrumpe, continua desde donde quedo

## Limitaciones conocidas

* Tucuman (provincia 90) y Tierra del Fuego (provincia 94) devuelven datos
  incompletos debido a un error en la distribucion oficial de las bases REDATAM
  por parte del INDEC. Este problema no es del paquete. Se recomienda reportarlo
  a censo2022@indec.gob.ar.
