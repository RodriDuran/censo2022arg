# =============================================================================
# censo_config.R
# Configuracion, rutas y funciones de soporte del paquete censo2022arg
#
# Copyright (C) 2025 Rodrigo Javier Duran
# INENCO - CONICET / Universidad Nacional de Salta
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# If you use this package in your research, please cite it as:
#   Duran, R. J. (2026). censo2022arg: Herramientas para el analisis del
#   Censo Argentino 2022. INENCO - CONICET/UNSa.
#   https://doi.org/10.5281/zenodo.19422784
# =============================================================================

#' @importFrom redatamx redatam_open redatam_close redatam_entities redatam_variables redatam_query redatam_run
#' @importFrom stats ave setNames
#' @importFrom utils download.file flush.console read.csv unzip write.csv
#' @useDynLib censo2022arg, .registration=TRUE
NULL

# =============================================================================
# FUNCION INTERNA: Mensaje de bienvenida
# Se muestra cada vez que el usuario llama a censo_configurar() o censo_info().
# =============================================================================
.censo_bienvenida <- function() {
  cat("\n")
  cat("+------------------------------------------------------------------+\n")
  cat("|              censo2022arg - Censo Argentina 2022                 |\n")
  cat("|    Extraccion y procesamiento de microdatos desde REDATAMX       |\n")
  cat("+------------------------------------------------------------------+\n")
  cat("\n")
  cat("Bienvenido/a. Este paquete permite acceder a los microdatos del\n")
  cat("Censo Nacional de Poblacion, Hogares y Viviendas 2022 de Argentina\n")
  cat("desde las bases REDATAMX distribuidas oficialmente por el INDEC.\n")
  cat("\n")
  cat("----------------------------------------------------------------------------\n")
  cat("    Algunas cosas que este paquete te permite hacer\n")
  cat("----------------------------------------------------------------------------\n")
  cat("\n")
  cat("  * Extraer microdatos completos (persona -> hogar -> vivienda)\n")
  cat("  * Filtrar por provincia, departamento, fraccion o radio censal\n")
  cat("  * Trabajar los datos con dplyr, ggplot2, tidyr y todo tidyverse\n")
  cat("  * Etiquetar automaticamente variables y valores (diccionarios oficiales INDEC)\n")
  cat("  * Exportar a CSV, SPSS (.sav), SAS (.sas7bdat) o Parquet\n")
  cat("  * Analizar sin necesidad de conocer la sintaxis SPC de REDATAM\n")
  cat("\n")
  cat("---------------------------------------------------------------------\n")
  cat("  Marco legal y atribuciones\n")
  cat("---------------------------------------------------------------------\n")
  cat("\n")
  cat("   Datos del censo\n")
  cat("  Los datos del Censo 2022 estan protegidos por la Ley N 17.622\n")
  cat("  de secreto estadistico. Su uso esta permitido exclusivamente\n")
  cat("  con fines estadisticos y de investigacion. Los datos individuales\n")
  cat("  no pueden ser utilizados para identificar personas.\n")
  cat("  Fuente oficial: INDEC - www.indec.gob.ar\n")
  cat("\n")
  cat("   Distribucion de datos\n")
  cat("  Este paquete NO DISTRIBUYE datos del censo. Solo proporciona\n")
  cat("  herramientas para acceder a las bases oficiales que el usuario\n")
  cat("  debe descargar directamente desde el INDEC.\n")
  cat("\n")
  cat("   Software Base\n")
  cat("  * El motor REDATAM es desarrollado por CELADE (CEPAL - Naciones Unidas)\n")
  cat("    y se distribuye de manera gratuita como software propietario.\n")
  cat("  * El paquete 'redatamx' (Jaime Salvador) es el wrapper que conecta R\n")
  cat("    con el motor REDATAM y se distribuye bajo licencia GPL (>= 3).\n")
  cat("\n")
  cat("   Autoria\n")
  cat("  * censo2022arg: Rodrigo Duran (INENCO/UNSa/CONICET)\n")
  cat("  * redatamx: Jaime Salvador (IdeasyBits)\n")
  cat("  * Motor REDATAM: CELADE (CEPAL)\n")
  cat("\n")
  cat("---------------------------------------------------------------------\n")
  cat("    Pasos para comenzar\n")
  cat("---------------------------------------------------------------------\n")
  cat("  1. censo_configurar('/ruta/datos')  # elegir donde guardar los datos\n")
  cat("  2. censo_verificar_engine()         # preparar el motor de extraccion\n")
  cat("  3. censo_descargar()                # descargar bases del INDEC\n")
  cat("  4. extraer_redatam()                # extraer microdatos por provincia\n")
  cat("  5. censo_etiquetar()                # aplicar etiquetas a las variables\n")
  cat("  6. censo_leer(base = 'Personas')    # leer y analizar los datos\n")
  cat("\n")
  cat("---------------------------------------------------------------------\n")
  cat("    Ayuda\n")
  cat("---------------------------------------------------------------------\n")
  cat("  censo_info()   # ver estado actual de la configuracion\n")
  cat("  GitHub: []\n")
  cat("---------------------------------------------------------------------\n")
  cat("\n")
}


# =============================================================================
# FUNCION INTERNA: Localizar archivos de datos incluidos en el paquete
#
# Los archivos de control (totales oficiales del INDEC por provincia)
# se distribuyen dentro del paquete en inst/extdata/. Esta funcion los
# localiza tanto en modo instalado (system.file) como en modo desarrollo.
# =============================================================================
.censo_extdata <- function(archivo) {
  # Buscar en la instalacion formal del paquete
  ruta <- system.file("extdata", archivo, package = "censo2022arg")

  # Si no se encuentra (modo desarrollo), buscar relativo al script actual
  if (nchar(ruta) == 0)
    ruta <- file.path(
      dirname(normalizePath(sys.frames()[[1]]$ofile, mustWork = FALSE)),
      "inst", "extdata", archivo
    )

  if (!file.exists(ruta))
    stop("Archivo de control no encontrado: ", archivo,
         "\nVerifica la instalacion del paquete.")
  ruta
}


# =============================================================================
# FUNCION: censo_configurar()
# =============================================================================

#' Configurar el directorio de datos del Censo 2022
#'
#' @description
#' Define el directorio donde se guardaran todos los archivos del censo:
#' bases REDATAM, metadatos y microdatos extraidos. Es el primer paso
#' antes de usar cualquier otra funcion del paquete.
#'
#' El directorio puede estar en cualquier ubicacion - disco interno,
#' externo o de red. El paquete en si se instala en la libreria de R
#' del sistema; los datos quedan donde usted elija.
#'
#' Al ejecutarse, la funcion crea automaticamente la estructura de
#' carpetas necesaria y copia los archivos de control oficial del INDEC.
#'
#' @param dir Ruta al directorio raiz de datos. Si no se especifica,
#'   se usa la ubicacion por defecto del sistema
#'   (\code{tools::R_user_dir("censo2022arg", "cache")}).
#' @param persistent Logico. Si \code{TRUE}, guarda la configuracion en
#'   \code{.Rprofile} para que el directorio quede disponible en todas
#'   las sesiones futuras sin necesidad de volver a configurar.
#'   Default \code{FALSE}.
#'
#' @return La ruta configurada (invisible).
#'
#' @examples
#' \dontrun{
#' # Configurar para esta sesion unicamente
#' censo_configurar("/home/usuario/mis_datos/censo2022")
#'
#' # Configurar y guardar para todas las sesiones futuras
#' censo_configurar("/home/usuario/mis_datos/censo2022", persistent = TRUE)
#'
#' # En Windows
#' censo_configurar("D:/Datos/Censo2022", persistent = TRUE)
#'
#' # En un disco externo (Mac/Linux)
#' censo_configurar("/Volumes/MiDisco/Censo2022", persistent = TRUE)
#' }
#'
#' @seealso \code{\link{censo_info}}, \code{\link{censo_descargar}},
#'   \code{\link{censo_verificar_engine}}
#' @export
censo_configurar <- function(
    dir        = NULL,
    persistent = FALSE
) {
  .censo_bienvenida()

  # Si no se especifica directorio, usar la ubicacion estandar del sistema
  if (is.null(dir)) {
    dir <- tools::R_user_dir("censo2022arg", which = "cache")
    cat("[INFO] No se especifico directorio. Usando ubicacion por defecto:\n")
    cat("[INFO]", dir, "\n\n")
  }

  # Normalizar la ruta (resolver ~, ., .., etc.)
  dir <- normalizePath(dir, mustWork = FALSE)

  # Crear la estructura de carpetas del paquete
  # Esta estructura replica la organizacion del INDEC y facilita la
  # orientacion del usuario en los archivos descargados.
  subdirs <- c(
    file.path(dir, "bases", "Base_VP"),       # Base viviendas particulares
    file.path(dir, "bases", "Base_PO_A_IG"),  # Base pueblos originarios e identidad de genero
    file.path(dir, "bases", "Base_VC_PSC"),   # Base viviendas colectivas
    file.path(dir, "metadatos"),              # Diccionarios y documentacion
    file.path(dir, "microdatos", "provincias"), # Datos extraidos por provincia
    file.path(dir, "microdatos", "logs")      # Registros de extraccion
  )
  for (d in subdirs) dir.create(d, recursive = TRUE, showWarnings = FALSE)

  # Registrar el directorio en la sesion actual
  options(censo2022.dir = dir)

  # Copiar archivos de control oficial del INDEC
  # Estos archivos contienen los totales de poblacion, hogares y viviendas
  # publicados por el INDEC, que se usan para verificar la integridad de
  # la extraccion. Se distribuyen con el paquete y se copian aqui.
  dir_ctrl <- file.path(dir, "metadatos", "Control de universos")
  dir.create(dir_ctrl, recursive = TRUE, showWarnings = FALSE)
  for (csv in c("control_poblacion.csv", "control_hogares.csv", "control_viviendas.csv")) {
    destino <- file.path(dir_ctrl, csv)
    if (!file.exists(destino)) {
      tryCatch(
        file.copy(.censo_extdata(csv), destino),
        error = function(e) cat("[WARN]  No se pudo copiar:", csv, "\n")
      )
    }
  }

  cat("[OK]  Directorio configurado:\n")
  cat("      ", dir, "\n\n")

  # Persistencia entre sesiones
  if (persistent) {
    rprofile     <- normalizePath("~/.Rprofile", mustWork = FALSE)
    dir_portable <- normalizePath(dir, winslash = "/", mustWork = FALSE)
    linea        <- sprintf('\noptions(censo2022.dir = "%s")\n', dir_portable)

    # Actualizar .Rprofile: eliminar configuracion anterior si existe
    if (file.exists(rprofile)) {
      contenido <- readLines(rprofile)
      contenido <- contenido[!grepl("censo2022.dir", contenido)]
      writeLines(contenido, rprofile)
    }
    cat(linea, file = rprofile, append = TRUE)
    cat("[OK]  Configuracion guardada. El directorio estara disponible\n")
    cat("      en todas las sesiones futuras sin necesidad de reconfigurar.\n\n")
  } else {
    cat("[!]   Esta configuracion es valida solo para la sesion actual.\n")
    cat("      Para que persista entre sesiones, ejecuta:\n\n")
    cat("        censo_configurar('", dir, "', persistent = TRUE)\n\n", sep = "")
  }

  # Orientar al usuario sobre el siguiente paso
  if (!file.exists(file.path(dir, "bases", "Base_VP", "cpv2022.rxdb"))) {
    cat("-- Proximo paso --------------------------------------------------\n")
    cat("  Las bases del censo no estan en el directorio configurado.\n\n")
    cat("  Para descargarlas automaticamente desde el INDEC:\n\n")
    cat("    censo_descargar()\n\n")
    cat("  Si ya las descargo manualmente, puede usarlas sin moverlas\n")
    cat("  pasando las rutas directamente a extraer_redatam().\n")
    cat("  Consulte: ?extraer_redatam\n")
    cat("-----------------------------------------------------------------\n\n")
  }

  invisible(dir)
}


# =============================================================================
# FUNCION INTERNA: censo_dir()
# Devuelve el directorio raiz configurado. Todas las funciones del paquete
# que necesitan acceder a archivos del censo llaman a esta funcion.
# Si el directorio no esta configurado, indica claramente como hacerlo.
# =============================================================================
#' @keywords internal
censo_dir <- function() {
  d <- getOption("censo2022.dir")

  if (is.null(d))
    stop(
      "Directorio no configurado.\n",
      "Ejecuta primero: censo_configurar('/ruta/a/tus/datos')\n",
      "Para mas informacion: censo_info()"
    )

  if (!dir.exists(d))
    stop(
      "El directorio configurado no existe: ", d, "\n",
      "Verifica la ruta o ejecuta censo_configurar() nuevamente."
    )
  d
}


# =============================================================================
# FUNCIONES INTERNAS: Rutas estandar del paquete
#
# Cada funcion devuelve la ruta a un componente especifico de la estructura
# de directorios. Se construyen siempre a partir de censo_dir(), de modo
# que si el usuario cambia el directorio raiz, todo se actualiza solo.
# =============================================================================

#' @keywords internal
censo_dir_bases      <- function() file.path(censo_dir(), "bases")

#' @keywords internal
censo_dir_vp         <- function() file.path(censo_dir(), "bases", "Base_VP")

#' @keywords internal
censo_dir_po         <- function() file.path(censo_dir(), "bases", "Base_PO_A_IG")

#' @keywords internal
censo_dir_vc         <- function() file.path(censo_dir(), "bases", "Base_VC_PSC")

#' @keywords internal
censo_dir_metadatos  <- function() file.path(censo_dir(), "metadatos")

#' @keywords internal
censo_dir_dicc       <- function() file.path(censo_dir(), "metadatos", "Diccionarios para Redatam")

#' @keywords internal
censo_dir_control    <- function() file.path(censo_dir(), "metadatos", "Control de universos")

#' @keywords internal
censo_dir_microdatos <- function() file.path(censo_dir(), "microdatos")

#' @keywords internal
censo_dir_provincias <- function() file.path(censo_dir(), "microdatos", "provincias")

#' @keywords internal
censo_dir_logs       <- function() file.path(censo_dir(), "microdatos", "logs")


# =============================================================================
# FUNCIONES INTERNAS: Rutas a archivos especificos
#
# Devuelven la ruta completa a cada archivo clave. Se usan internamente
# para verificar si los archivos estan presentes y para abrirlos.
# =============================================================================

#' @keywords internal
censo_rxdb_vp <- function() file.path(censo_dir_vp(), "cpv2022.rxdb")

#' @keywords internal
censo_rxdb_po <- function() file.path(censo_dir_po(), "cpv2022.rxdb")

#' @keywords internal
censo_rxdb_vc <- function() file.path(censo_dir_vc(), "cpv2022col.rxdb")

#' @keywords internal
censo_xls_vp <- function() file.path(censo_dir_dicc(),
                                     "c2022-diccionario_viviendas_particulares.xlsx")

#' @keywords internal
censo_xls_vc <- function() file.path(censo_dir_dicc(),
                                     "c2022-diccionario_viviendas_colectivas_poblacion_situacion_calle.xlsx")

#' @keywords internal
censo_xls_po <- function() file.path(censo_dir_dicc(),
                                     "c2022-diccionario_pueblos_originarios_afrodescendientes_genero.xlsx")


# =============================================================================
# censo_verificar_engine()
# Verificacion y guia de preparacion del motor REDATAM
#
# El motor libredengine tiene un limite interno de extraccion que difiere
# entre sistemas operativos porque los compiladores (GCC en Linux/Mac,
# MSVC en Windows) generan codigo binario distinto para la misma constante.
# Por eso los offsets y bytes de referencia son diferentes en cada plataforma.
#
# Valores verificados experimentalmente:
#
#   Linux / Mac
#     Offset:           0x956BC5
#     Bytes originales: 40 A6 9F 02 C3 66 0F 1F
#     Bytes parchados:  00 00 00 00 00 00 00 00
#
#   Windows
#     Offset:           0x10CC61
#     Bytes originales: 64 00 00 00
#     Bytes parchados:  FF FF FF 7F  (retorna 2.147.483.647)
# =============================================================================

#' Verificar el estado del motor de extraccion REDATAM
#'
#' @description
#' Verifica si el motor de extraccion esta correctamente preparado para
#' trabajar con el Censo 2022. Si no lo esta, muestra las instrucciones
#' paso a paso para prepararlo segun su sistema operativo.
#'
#' \strong{Por que es necesario este paso?}
#'
#' El motor REDATAM (distribuido con el paquete \code{redatamx}) tiene
#' un limite interno que restringe la extraccion a 100 registros por
#' consulta. Este limite fue disenado para uso interactivo del software
#' REDATAM, no para la extraccion masiva de microdatos. Para poder
#' extraer los 44 millones de registros del censo, es necesario ampliar
#' ese limite aplicando una modificacion puntual al archivo del motor.
#'
#' Esta modificacion es de bajo nivel (unos pocos bytes en el binario
#' compilado) y no afecta ninguna otra funcionalidad. Debe realizarse
#' una sola vez, y puede revertirse en cualquier momento usando la
#' copia de seguridad que la funcion indica crear.
#'
#' @return Invisible \code{TRUE} si el motor esta listo, \code{FALSE}
#'   si requiere preparacion.
#'
#' @examples
#' \dontrun{
#' censo_verificar_engine()
#' }
#'
#' @seealso \code{\link{censo_configurar}}, \code{\link{extraer_redatam}}
#' @export
censo_verificar_engine <- function() {

  es_windows <- .Platform$OS.type == "windows"

  # Nombre del binario segun sistema operativo
  so_nombre <- if (es_windows) {
    "libredengine-1.2.1-final.dll"
  } else {
    "libredengine-1.2.1-final.so"
  }

  so_path <- system.file("redengine", so_nombre, package = "redatamx")

  if (!nzchar(so_path) || !file.exists(so_path)) {
    cat("[ERROR] No se encontro el motor REDATAM.\n")
    cat("[INFO]  Verifique que el paquete redatamx esta instalado:\n\n")
    cat("          install.packages('redatamx')\n\n")
    return(invisible(FALSE))
  }

  cat("[INFO] Motor encontrado:\n")
  cat("      ", so_path, "\n\n")

  # ------------------------------------------------------------------
  # Parametros del parche por sistema operativo.
  # Los offsets y bytes fueron determinados experimentalmente analizando
  # el binario compilado en cada plataforma con la version 1.2.1 del motor.
  # ------------------------------------------------------------------
  if (es_windows) {

    # Windows (compilado con MSVC):
    # La funcion view_max_rows compila como B8 64 00 00 00 C3
    # (MOV EAX, 100; RET). El parche reemplaza los 4 bytes del operando
    # 64 00 00 00 por FF FF FF 7F para retornar 2.147.483.647.
    OFFSET       <- 0x10CC61
    BYTES_ORIG   <- as.raw(c(0x64, 0x00, 0x00, 0x00))
    BYTES_PARCHE <- as.raw(c(0xFF, 0xFF, 0xFF, 0x7F))
    N_BYTES      <- 4L

  } else {

    # Linux y Mac (compilado con GCC):
    # El parche reemplaza 8 bytes que codifican el limite de 100 filas
    # con ceros, eliminando la restriccion.
    OFFSET       <- 0x956BC5
    BYTES_ORIG   <- as.raw(c(0x40, 0xA6, 0x9F, 0x02, 0xC3, 0x66, 0x0F, 0x1F))
    BYTES_PARCHE <- as.raw(rep(0x00, 8))
    N_BYTES      <- 8L
  }

  # Leer los bytes actuales en el offset conocido
  con            <- file(so_path, "rb")
  seek(con, OFFSET)
  bytes_actuales <- readBin(con, "raw", n = N_BYTES)
  close(con)

  ya_parchado <- identical(bytes_actuales, BYTES_PARCHE)
  es_original <- identical(bytes_actuales, BYTES_ORIG)

  # Motor ya preparado correctamente
  if (ya_parchado) {
    cat("[OK]   El motor esta correctamente preparado.\n")
    cat("[OK]   El limite de extraccion ha sido ampliado.\n")
    return(invisible(TRUE))
  }

  # Version desconocida: puede ser una actualizacion de redatamx
  if (!es_original) {
    cat("[AVISO] Los bytes del motor no coinciden con la version conocida.\n")
    cat("[AVISO] Bytes encontrados:",
        paste(toupper(as.character(bytes_actuales)), collapse = " "), "\n")
    cat("[AVISO] Es posible que el paquete redatamx haya sido actualizado.\n\n")
    cat("[INFO]  Por favor, reporte este mensaje en:\n")
    cat("          https://github.com/RodriDuran/censo2022arg/issues\n\n")
    return(invisible(FALSE))
  }

  # Motor sin preparar: mostrar instrucciones segun sistema operativo
  so_bak <- paste0(so_path, ".bak")

  cat("[AVISO] El motor no esta preparado para la extraccion completa.\n")
  cat("[AVISO] Sin este paso, solo se pueden extraer 100 registros.\n\n")
  cat("  A continuacion se indican los pasos para preparar el motor.\n")
  cat("  Solo necesita hacerlo una vez.\n\n")

  if (es_windows) {

    cat("--- Instrucciones para Windows -----------------------------------\n")
    cat("  1. Cierre RStudio y toda instancia de R completamente.\n")
    cat("  2. Abra PowerShell COMO ADMINISTRADOR:\n")
    cat("     (clic derecho sobre PowerShell -> 'Ejecutar como administrador')\n\n")
    cat("  PASO 1 - Crear copia de seguridad:\n\n")
    cat(sprintf('  Copy-Item "%s" `\n          "%s"\n\n', so_path, so_bak))
    cat("  PASO 2 - Preparar el motor (copie y pegue exactamente):\n\n")
    cat(sprintf(
      '  $path = "%s"
  $bytes = [System.IO.File]::ReadAllBytes($path)
  $bytes[0x%X] = 0xFF
  $bytes[0x%X] = 0xFF
  $bytes[0x%X] = 0xFF
  $bytes[0x%X] = 0x7F
  [System.IO.File]::WriteAllBytes($path, $bytes)
  Write-Host "Motor preparado correctamente"
',
      so_path,
      OFFSET, OFFSET + 1L, OFFSET + 2L, OFFSET + 3L
    ))
    cat("\n  PASO 3 - Abra R y verifique:\n\n")
    cat("    censo_verificar_engine()\n\n")
    cat("  Si algo no funciona, recupere la copia de seguridad:\n\n")
    cat(sprintf('  Copy-Item "%s" `\n          "%s"\n', so_bak, so_path))

  } else {

    cat("-- Instrucciones para Linux / Mac ------------------------------\n")
    cat("  Abra una terminal y ejecute los siguientes comandos:\n\n")
    cat("  PASO 1 - Crear copia de seguridad:\n\n")
    cat(sprintf('  cp "%s" \\\n     "%s"\n\n', so_path, so_bak))
    cat("  PASO 2 - Preparar el motor (copie y pegue exactamente):\n\n")
    cat(sprintf(
      '  printf \'\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\' | \\
    dd of="%s" \\
    bs=1 seek=$((0x%X)) conv=notrunc
', so_path, OFFSET))
    cat("\n  PASO 3 - Verifique desde R:\n\n")
    cat("    censo_verificar_engine()\n\n")
    cat("  Si algo no funciona, recupere la copia de seguridad:\n\n")
    cat(sprintf('  cp "%s" \\\n     "%s"\n', so_bak, so_path))
  }

  cat("------------------------------------------------------------------\n\n")
  invisible(FALSE)
}


# =============================================================================
# FUNCION: censo_info()
# =============================================================================

#' Mostrar el estado actual de la configuracion
#'
#' @description
#' Presenta un resumen del estado del paquete: directorio configurado,
#' archivos disponibles, provincias extraidas y proximos pasos sugeridos.
#' Tambien verifica si el motor de extraccion esta correctamente preparado.
#'
#' Es util para orientarse al retomar el trabajo despues de un tiempo
#' o para diagnosticar problemas de configuracion.
#'
#' @return La ruta del directorio configurado (invisible), o \code{NULL}
#'   si no hay configuracion activa.
#'
#' @examples
#' \dontrun{
#' censo_info()
#' }
#'
#' @seealso \code{\link{censo_configurar}}, \code{\link{censo_descargar}}
#' @export
censo_info <- function() {

  .censo_bienvenida()

  # Intentar obtener el directorio configurado
  d <- tryCatch(censo_dir(), error = function(e) NULL)

  if (is.null(d)) {
    cat("-- Estado: SIN CONFIGURAR ----------------------------------------\n")
    cat("  No hay un directorio de datos configurado.\n\n")
    cat("  Para comenzar, ejecute:\n\n")
    cat("    censo_configurar('/ruta/donde/guardar/los/datos')\n\n")
    cat("  Puede elegir cualquier directorio en su equipo o disco externo.\n")
    cat("-----------------------------------------------------------------\n\n")
    return(invisible(NULL))
  }

  cat("-- Directorio raiz -----------------------------------------------\n")
  cat("  ", d, "\n\n")

  # Verificar presencia de cada carpeta de la estructura del paquete
  cat("-- Estructura de directorios -------------------------------------\n")
  componentes <- list(
    "bases/Base_VP"                       = censo_dir_vp(),
    "bases/Base_PO_A_IG"                  = censo_dir_po(),
    "bases/Base_VC_PSC"                   = censo_dir_vc(),
    "metadatos/Diccionarios para Redatam" = censo_dir_dicc(),
    "metadatos/Control de universos"      = censo_dir_control(),
    "microdatos/provincias"               = censo_dir_provincias(),
    "microdatos/logs"                     = censo_dir_logs()
  )
  for (nom in names(componentes)) {
    ruta   <- componentes[[nom]]
    existe <- dir.exists(ruta)
    if (existe) {
      n <- length(list.files(ruta, recursive = FALSE))
      cat(sprintf("  %-42s OK  (%d archivos)\n", nom, n))
    } else {
      cat(sprintf("  %-42s [no existe]\n", nom))
    }
  }

  # Verificar presencia de los archivos de bases y diccionarios
  cat("\n-- Archivos clave ------------------------------------------------\n")
  archivos <- list(
    "Base VP  (cpv2022.rxdb)"       = censo_rxdb_vp(),
    "Base PO  (cpv2022.rxdb)"       = censo_rxdb_po(),
    "Base VC  (cpv2022col.rxdb)"    = censo_rxdb_vc(),
    "Diccionario VP (XLS)"          = censo_xls_vp(),
    "Diccionario VC (XLS)"          = censo_xls_vc()
  )
  bases_rxdb_ok <- file.exists(censo_rxdb_vp()) &&
    file.exists(censo_rxdb_po()) &&
    file.exists(censo_rxdb_vc())
  bases_xls_ok  <- file.exists(censo_xls_vp()) &&
    file.exists(censo_xls_vc())
  for (nom in names(archivos)) {
    existe <- file.exists(archivos[[nom]])
    cat(sprintf("  %-30s %s\n", nom, if (existe) "OK" else "[no encontrado]"))
  }

  # Listar provincias ya extraidas
  cat("\n-- Microdatos extraidos ------------------------------------------\n")
  dir_prov <- censo_dir_provincias()
  provs    <- character(0)
  if (dir.exists(dir_prov)) {
    provs  <- list.dirs(dir_prov, recursive = FALSE, full.names = FALSE)
    provs  <- provs[grepl("^[0-9]{2}_", provs)]
    n_prov <- length(provs)
    cat(sprintf("  Provincias extraidas: %d / 24\n", n_prov))
    if (n_prov > 0) for (p in provs) cat("    *", p, "\n")
    if (n_prov < 24)
      cat(sprintf("  Provincias pendientes: %d\n", 24 - n_prov))
  }

  # Sugerir el proximo paso segun el estado actual
  cat("\n-- Proximo paso sugerido -----------------------------------------\n")
  if (!bases_rxdb_ok) {
    cat("  Las bases del censo no estan descargadas. Ejecuta:\n\n")
    cat("    censo_descargar()\n\n")
    cat("  Si ya las descargo manualmente, puede pasarlas directamente\n")
    cat("  a extraer_redatam(). Consulte: ?extraer_redatam\n")
  } else if (!bases_xls_ok) {
    cat("  Las bases estan disponibles pero faltan los diccionarios de variables.\n")
    cat("  Son necesarios para etiquetar los microdatos. Ejecute:\n\n")
    cat("    censo_descargar(que = 'metadatos')\n")
  } else if (length(provs) == 0) {
    cat("  Las bases estan disponibles. Para extraer los microdatos:\n\n")
    cat("    extraer_redatam()                # todas las provincias\n")
    cat("    extraer_redatam(provincias = 34) # solo Formosa (prueba rapida)\n")
  } else if (length(provs) < 24) {
    cat("  Extraccion en curso. Para continuar:\n\n")
    cat("    extraer_redatam()  # retoma automaticamente donde quedo\n")
  } else {
    cat("  Todas las provincias estan extraidas.\n\n")
    cat("    censo_etiquetar()              # aplicar etiquetas a las variables\n")
    cat("    censo_leer(base = 'Personas')  # leer y analizar los datos\n")
  }
  cat("-----------------------------------------------------------------\n\n")

  # Verificar estado del motor al final del informe
  cat("-- Estado del motor de extraccion --------------------------------\n")
  censo_verificar_engine()

  invisible(d)
}


# =============================================================================
# FUNCION INTERNA: .descomprimir_indec()
#
# Los archivos ZIP del INDEC usan codificacion CP850 (estandar DOS/Windows
# antiguo) para los nombres de archivo con tildes y n. Esto impide extraerlos
# correctamente con unzip() estandar en cualquier sistema operativo.
#
# Solucion: se extraen todos los archivos planos (sin estructura de
# directorios) a un directorio temporal, se renombran los archivos con
# nombres corruptos usando un mapeo fijo conocido, y finalmente se mueven
# a sus carpetas correctas dentro del directorio de metadatos.
#
# Este comportamiento es especifico de los ZIPs del INDEC y no requiere
# ninguna dependencia externa.
# =============================================================================
.descomprimir_indec <- function(zip_path, destino) {

  tmp_dir <- normalizePath(
    file.path(tempdir(), "censo_meta_tmp"),
    winslash = "/", mustWork = FALSE
  )
  destino <- normalizePath(destino, winslash = "/", mustWork = FALSE)
  dir.create(tmp_dir, showWarnings = FALSE, recursive = TRUE)
  on.exit(unlink(tmp_dir, recursive = TRUE))

  # Extraer todos los archivos ignorando la estructura de carpetas del ZIP.
  # junkpaths = TRUE evita que los nombres corruptos de las carpetas
  # interfieran con la extraccion.
  info     <- unzip(zip_path, list = TRUE)
  archivos <- info$Name[info$Length > 0]  # excluir entradas de directorio

  for (arch in archivos) {
    tryCatch(
      unzip(zip_path, files = arch, exdir = tmp_dir, junkpaths = TRUE),
      error   = function(e) NULL,
      warning = function(w) NULL
    )
  }

  # Corregir nombres de archivo con codificacion CP850 -> UTF-8.
  # Los bytes corruptos corresponden a: \xrx82 = e, \xa2 = o, \xa1 = i
  mapeo_nombres <- c(
    "c2022-diccionario_pueblos_originarios_afrodescendientes_g\x82nero.xlsx"      = "c2022-diccionario_pueblos_originarios_afrodescendientes_genero.xlsx",
    "c2022-diccionario_viviendas_colectivas_poblaci\xa2n_situaci\xa2n_calle.xlsx" = "c2022-diccionario_viviendas_colectivas_poblacion_situacion_calle.xlsx",
    "Redatam_unidades_geoestad\xa1sticas.pdf"                                     = "Redatam_unidades_geoestadisticas.pdf"
  )
  for (nom_corrupto in names(mapeo_nombres)) {
    ruta_orig <- file.path(tmp_dir, nom_corrupto)
    ruta_dest <- file.path(tmp_dir, mapeo_nombres[[nom_corrupto]])
    if (file.exists(ruta_orig)) file.rename(ruta_orig, ruta_dest)
  }

  # Mover cada archivo a su subcarpeta correcta dentro de metadatos/
  # El mapeo replica la estructura de carpetas del ZIP original del INDEC.
  mapeo_destino <- list(
    "Redatam_control_de_universos.pdf"                                      = "Control de universos",
    "Censo2022_cuestionario_viviendas_colectivas.pdf"                       = "Cuestionarios",
    "Censo2022_cuestionario_viviendas_particulares.pdf"                     = "Cuestionarios",
    "Redatam_definiciones_de_la_base_de_datos.pdf"                          = "Definiciones",
    "Redatam_definicion_de_indicadores.pdf"                                 = "Definiciones",
    "c2022-diccionario_pueblos_originarios_afrodescendientes_genero.xlsx"   = "Diccionarios para Redatam",
    "c2022-diccionario_viviendas_colectivas_poblacion_situacion_calle.xlsx" = "Diccionarios para Redatam",
    "c2022-diccionario_viviendas_particulares.xlsx"                         = "Diccionarios para Redatam",
    "Redatam_unidades_geoestadisticas.pdf"                                  = "Unidades geoestadisticas",
    "c2022_codigos_aglomerados.xlsx"                                        = "Unidades geoestadisticas/Codigos geograficos",
    "c2022_codigos_departamentos.xlsx"                                      = "Unidades geoestadisticas/Codigos geograficos",
    "c2022_codigos_jurisdicciones.xlsx"                                     = "Unidades geoestadisticas/Codigos geograficos",
    "c2022_codigos_localidades.xlsx"                                        = "Unidades geoestadisticas/Codigos geograficos",
    "c2022_codigos_paises.xlsx"                                             = "Unidades geoestadisticas/Codigos geograficos",
    "c2022_gobiernos_locales.xlsx"                                          = "Unidades geoestadisticas/Codigos geograficos"
  )

  n_ok <- 0L; n_warn <- 0L
  for (nom in names(mapeo_destino)) {
    subdir    <- file.path(destino, mapeo_destino[[nom]])
    dir.create(subdir, recursive = TRUE, showWarnings = FALSE)
    ruta_orig <- file.path(tmp_dir, nom)
    ruta_dest <- file.path(subdir, nom)
    if (file.exists(ruta_orig)) {
      file.copy(ruta_orig, ruta_dest, overwrite = TRUE)
      cat("[OK]   ", nom, "\n")
      n_ok <- n_ok + 1L
    } else {
      cat("[AVISO] No encontrado:", nom, "\n")
      n_warn <- n_warn + 1L
    }
  }

  cat(sprintf("[INFO]  %d archivos extraidos", n_ok))
  if (n_warn > 0) cat(sprintf(", %d no encontrados", n_warn))
  cat("\n")
}


# =============================================================================
# FUNCION: censo_descargar()
# =============================================================================

#' Descargar bases y documentacion del Censo 2022 desde el INDEC
#'
#' @description
#' Descarga automaticamente desde el portal oficial del INDEC las bases
#' de datos REDATAM, los diccionarios de variables, los cuestionarios y
#' la documentacion metodologica del Censo 2022.
#'
#' Los archivos se guardan en la estructura de directorios configurada
#' previamente con \code{censo_configurar()}.
#'
#' @param que Character. Que descargar. Opciones:
#'   \itemize{
#'     \item \code{"todo"} (default): descarga todo lo disponible
#'     \item \code{"bases"}: solo las bases REDATAM (~500 MB)
#'     \item \code{"metadatos"}: diccionarios de variables y documentacion
#'     \item \code{"cuestionarios"}: formularios del censo en PDF
#'     \item \code{"metodologia"}: documentos metodologicos en PDF
#'   }
#'   Se pueden combinar: \code{c("cuestionarios", "metodologia")}.
#' @param overwrite Logico. Si \code{TRUE}, vuelve a descargar aunque
#'   el archivo ya exista. Default \code{FALSE}.
#'
#' @details
#' La descarga de las bases puede demorar varios minutos dependiendo de
#' la conexion. Si el enlace no esta disponible, la funcion indica como
#' descargar los archivos manualmente desde el sitio del INDEC.
#'
#' @examples
#' \dontrun{
#' # Descargar todo (recomendado la primera vez)
#' censo_descargar()
#'
#' # Solo los metadatos (mas rapido, util para probar la configuracion)
#' censo_descargar(que = "metadatos")
#'
#' # Forzar re-descarga de las bases
#' censo_descargar(que = "bases", overwrite = TRUE)
#' }
#'
#' @seealso \code{\link{censo_configurar}}, \code{\link{extraer_redatam}}
#' @export
censo_descargar <- function(
    que       = "todo",
    overwrite = FALSE
) {
  que <- match.arg(que,
                   c("todo", "bases", "metadatos", "cuestionarios", "metodologia"),
                   several.ok = TRUE)
  if ("todo" %in% que) que <- c("bases", "metadatos", "cuestionarios", "metodologia")

  # Ampliar el timeout para archivos grandes (bases ~500 MB)
  # Se restaura automaticamente al salir de la funcion
  op_orig <- options(timeout = 3600)
  on.exit(options(op_orig), add = TRUE)

  # ---- Bases REDATAM --------------------------------------------------------
  if ("bases" %in% que) {
    cat("[INFO] === BASES REDATAM ===\n")
    destino <- censo_dir_bases()
    url     <- "https://www.indec.gob.ar/ftp/cuadros/poblacion/bases_censo2022_RedatamX.zip"

    ya_existe <- file.exists(censo_rxdb_vp()) &&
      file.exists(censo_rxdb_po()) &&
      file.exists(censo_rxdb_vc())

    if (ya_existe && !overwrite) {
      cat("[INFO]  Las bases ya estan descargadas.\n")
      cat("[INFO]  Use overwrite = TRUE para forzar la re-descarga.\n")
    } else {
      cat("[INFO]  Descargando bases (~500 MB, puede demorar)...\n")
      cat("[INFO]  URL:", url, "\n")
      zip_tmp <- tempfile(fileext = ".zip")
      tryCatch({
        download.file(url, zip_tmp, mode = "wb", method = "auto", quiet = FALSE)
        sz <- round(file.size(zip_tmp) / 1024 / 1024, 1)
        cat("[INFO]  Descargado:", sz, "MB - descomprimiendo...\n")
        unzip(zip_tmp, exdir = destino)
        file.remove(zip_tmp)
        cat("[OK]    Bases disponibles en:", destino, "\n")
      }, error = function(e) {
        cat("[ERROR]", conditionMessage(e), "\n")
        cat("[INFO]  El enlace puede no estar disponible en este momento.\n")
        cat("[INFO]  Descargue manualmente desde:\n")
        cat("[INFO]    https://www.indec.gob.ar/indec/web/Institucional-Indec-BasesDeDatos\n")
        cat("[INFO]  Descomprima el ZIP y copie las carpetas Base_VP, Base_PO_A_IG\n")
        cat("[INFO]  y Base_VC_PSC en:", destino, "\n")
      })
    }
  }

  # ---- Metadatos (diccionarios, definiciones, unidades geograficas) ---------
  if ("metadatos" %in% que) {
    cat("[INFO] === METADATOS ===\n")
    destino <- censo_dir_metadatos()
    url     <- "https://www.indec.gob.ar/ftp/cuadros/poblacion/metadatos_censo2022_redatam.zip"

    ya_existe <- file.exists(censo_xls_vp())

    if (ya_existe && !overwrite) {
      cat("[INFO]  Los metadatos ya estan descargados.\n")
      cat("[INFO]  Use overwrite = TRUE para forzar la re-descarga.\n")
    } else {
      cat("[INFO]  Descargando metadatos...\n")
      cat("[INFO]  URL:", url, "\n")
      zip_tmp <- tempfile(fileext = ".zip")
      tryCatch({
        download.file(url, zip_tmp, mode = "wb", method = "auto", quiet = FALSE)
        sz <- round(file.size(zip_tmp) / 1024 / 1024, 1)
        cat("[INFO]  Descargado:", sz, "MB - descomprimiendo...\n")
        .descomprimir_indec(zip_tmp, destino)  # manejo especial de encoding CP850
        file.remove(zip_tmp)
        cat("[OK]    Metadatos disponibles en:", destino, "\n")
      }, error = function(e) {
        cat("[ERROR]", conditionMessage(e), "\n")
        cat("[INFO]  El enlace puede no estar disponible en este momento.\n")
        cat("[INFO]  Descargue manualmente desde:\n")
        cat("[INFO]    https://www.indec.gob.ar/indec/web/Institucional-Indec-BasesDeDatos\n")
        cat("[INFO]  Descomprima el ZIP y copie el contenido en:", destino, "\n")
      })
    }
  }

  # ---- Cuestionarios (PDF) --------------------------------------------------
  if ("cuestionarios" %in% que) {
    cat("[INFO] === CUESTIONARIOS ===\n")
    destino <- file.path(censo_dir_metadatos(), "Cuestionarios")
    dir.create(destino, recursive = TRUE, showWarnings = FALSE)

    pdfs <- list(
      list(
        url     = "https://www.indec.gob.ar/ftp/cuadros/poblacion/Censo2022_cuestionario_viviendas_particulares.pdf",
        archivo = "Censo2022_cuestionario_viviendas_particulares.pdf"
      ),
      list(
        url     = "https://www.indec.gob.ar/ftp/cuadros/poblacion/Censo2022_cuestionario_viviendas_colectivas.pdf",
        archivo = "Censo2022_cuestionario_viviendas_colectivas.pdf"
      )
    )

    for (pdf in pdfs) {
      ruta_dest <- file.path(destino, pdf$archivo)
      if (file.exists(ruta_dest) && !overwrite) {
        cat("[INFO]  Ya existe:", pdf$archivo, "\n")
        next
      }
      tryCatch({
        download.file(pdf$url, ruta_dest, mode = "wb", quiet = TRUE)
        sz <- round(file.size(ruta_dest) / 1024 / 1024, 1)
        cat("[OK]   ", pdf$archivo, "(", sz, "MB)\n")
      }, error = function(e) {
        cat("[ERROR]", pdf$archivo, ":", conditionMessage(e), "\n")
        cat("[INFO]  Descargue manualmente desde:", pdf$url, "\n")
      })
    }
  }

  # ---- Documentacion metodologica (PDF) ------------------------------------
  if ("metodologia" %in% que) {
    cat("[INFO] === METODOLOGIA ===\n")
    destino <- file.path(censo_dir_metadatos(), "Definiciones")
    dir.create(destino, recursive = TRUE, showWarnings = FALSE)

    pdfs <- list(
      list(
        url     = "https://www.indec.gob.ar/ftp/cuadros/poblacion/censo2022_codificacion_preguntas_abiertas.pdf",
        archivo = "censo2022_codificacion_preguntas_abiertas.pdf"
      ),
      list(
        url     = "https://www.indec.gob.ar/ftp/cuadros/poblacion/sintesis_planificacion_censo_2022.pdf",
        archivo = "sintesis_planificacion_censo_2022.pdf"
      )
    )

    for (pdf in pdfs) {
      ruta_dest <- file.path(destino, pdf$archivo)
      if (file.exists(ruta_dest) && !overwrite) {
        cat("[INFO]  Ya existe:", pdf$archivo, "\n")
        next
      }
      tryCatch({
        download.file(pdf$url, ruta_dest, mode = "wb", quiet = TRUE)
        sz <- round(file.size(ruta_dest) / 1024 / 1024, 1)
        cat("[OK]   ", pdf$archivo, "(", sz, "MB)\n")
      }, error = function(e) {
        cat("[ERROR]", pdf$archivo, ":", conditionMessage(e), "\n")
        cat("[INFO]  Descargue manualmente desde:", pdf$url, "\n")
      })
    }
  }

  cat("\n[INFO] Descarga finalizada. Ejecute censo_info() para ver el estado.\n")
  invisible(NULL)
}
