# =============================================================================
# Etiquetar.R
# Etiquetado de variables en los microdatos extraidos del Censo 2022
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
# El proceso de extraccion genera archivos con nombres de columna en formato
# tecnico del motor REDATAM (ej: p01_0, edad_2). Este script aplica las
# etiquetas del diccionario oficial del INDEC para convertir esos nombres
# en nombres legibles (P01, EDAD) y transformar las variables categoricas
# en factores con sus etiquetas de valor.
#
# Funciones exportadas:
#   censo_etiquetar() - funcion principal, compatible con extraer_redatam()
#                       y extraer_dic(). Seleccion automatica de fuente de
#                       metadatos: xls > redatam > dic.
#
# Funciones internas:
#   construir_metadatos()            - extrae etiquetas del .rxdb via motor
#   construir_metadatos_subprocess() - idem en subproceso (gestion de RAM)
#   construir_metadatos_xls()        - extrae etiquetas desde XLS del INDEC
#   construir_metadatos_dic()        - extrae etiquetas desde cualquier
#                                      diccionario via redatam_variables()
#                                      sin etiquetas de valores
#   limpiar_nombre()                 - normaliza nombres de columna
# =============================================================================


# =============================================================================
# FUNCION INTERNA: construir_metadatos()
#
# Extrae del diccionario REDATAM (.rxdb) dos tipos de informacion:
#   var_labels: nombre descriptivo de cada variable
#   val_labels: codigos y etiquetas de cada categoria para variables
#               de tipo entero (las variables categoricas)
#
# Para obtener los valores de categoria se ejecuta una consulta FREQ
# sobre cada variable entera. Esto puede ser lento en bases grandes,
# razon por la que la fuente XLS es preferida por defecto.
#
# Se limita a 500 categorias por variable para evitar variables continuas
# que el motor clasifica como entero (ej: edad, ano de nacimiento).
# =============================================================================
construir_metadatos <- function(dic_path) {

  dic <- redatam_open(dic_path)
  on.exit(redatam_close(dic))

  # Obtener entidades de la jerarquia; usar lista por defecto si falla
  entidades <- tryCatch(
    redatam_entities(dic)$name,
    error = function(e) c("PROV", "DPTO", "FRAC", "RADIO", "VIVIENDA", "HOGAR", "PERSONA")
  )

  var_labels <- list()
  val_labels <- list()

  for (entidad in entidades) {
    vars <- tryCatch(redatam_variables(dic, entidad), error = function(e) NULL)
    if (is.null(vars) || nrow(vars) == 0) next

    for (i in seq_len(nrow(vars))) {
      nom      <- toupper(vars$name[i])
      etiqueta <- vars$label[i]
      tipo     <- vars$typeName[i]

      var_labels[[nom]] <- etiqueta

      # Para variables enteras, obtener las categorias con FREQ
      if (tipo == "integer") {
        tbl <- tryCatch({
          res <- redatam_query(dic, paste0("FREQ ", entidad, ".", vars$name[i]))
          if (is.data.frame(res)) res else res[[1]]
        }, error = function(e) NULL)
        if (!is.null(tbl) && nrow(tbl) > 1) {
          tbl <- tbl[!is.na(tbl[[1]]), , drop = FALSE]
          # Limitar a 500 categorias para excluir variables continuas
          if (nrow(tbl) <= 500) {
            val_labels[[nom]] <- data.frame(
              value = as.integer(tbl[[1]]),
              label = as.character(tbl[[2]]),
              stringsAsFactors = FALSE
            )
          }
        }
      }
    }
  }

  list(var_labels = var_labels, val_labels = val_labels)
}


# =============================================================================
# FUNCION INTERNA: construir_metadatos_subprocess()
#
# Ejecuta construir_metadatos() en un subproceso Rscript independiente.
# Esto es necesario porque abrir el diccionario REDATAM y ejecutar multiples
# consultas FREQ consume RAM que no se libera correctamente en la sesion
# principal. El subproceso libera toda esa memoria al terminar.
#
# El resultado se guarda temporalmente en disco como .rds y se recupera
# en la sesion principal.
# =============================================================================
construir_metadatos_subprocess <- function(dic_path, out_file) {

  script <- sprintf('
suppressMessages(library(redatamx))

construir_metadatos <- function(dic_path) {
  dic <- redatam_open(dic_path)
  on.exit(redatam_close(dic))
  entidades <- tryCatch(
    redatam_entities(dic)$name,
    error = function(e) c("PROV","DPTO","FRAC","RADIO","VIVIENDA","HOGAR","PERSONA")
  )
  var_labels <- list()
  val_labels <- list()
  for (entidad in entidades) {
    vars <- tryCatch(redatam_variables(dic, entidad), error = function(e) NULL)
    if (is.null(vars) || nrow(vars) == 0) next
    for (i in seq_len(nrow(vars))) {
      nom      <- toupper(vars$name[i])
      etiqueta <- vars$label[i]
      tipo     <- vars$typeName[i]
      var_labels[[nom]] <- etiqueta
      if (tipo == "integer") {
        tbl <- tryCatch({
          res <- redatam_query(dic, paste0("FREQ ", entidad, ".", vars$name[i]))
          if (is.data.frame(res)) res else res[[1]]
        }, error = function(e) NULL)
        if (!is.null(tbl) && nrow(tbl) > 1) {
          tbl <- tbl[!is.na(tbl[[1]]), , drop = FALSE]
          if (nrow(tbl) <= 500) {
            val_labels[[nom]] <- data.frame(
              value = as.integer(tbl[[1]]),
              label = as.character(tbl[[2]]),
              stringsAsFactors = FALSE
            )
          }
        }
      }
    }
  }
  list(var_labels = var_labels, val_labels = val_labels)
}

meta <- construir_metadatos("%s")
saveRDS(meta, "%s")
cat("OK", length(meta$var_labels), "variables\\n")
', dic_path, out_file)

  tmp_script <- tempfile(fileext = ".R")
  on.exit(unlink(tmp_script), add = TRUE)
  writeLines(script, tmp_script)
  output <- system(paste("Rscript", shQuote(tmp_script), "2>&1"), intern = TRUE, wait = TRUE)
  for (linea in output) message("[INFO]  [meta] ", linea)

  ret <- attr(output, "status")
  ret <- if (is.null(ret)) 0L else ret

  if (ret != 0 || !file.exists(out_file)) {
    message("[ERROR] No se pudieron construir los metadatos desde:", dic_path)
    return(NULL)
  }

  meta <- readRDS(out_file)
  file.remove(out_file)
  meta
}

# =============================================================================
# FUNCION INTERNA: construir_metadatos_dic()
#
# Extrae etiquetas de variables directamente desde cualquier diccionario
# REDATAM via redatam_variables(). No ejecuta consultas FREQ por lo que
# no obtiene etiquetas de valores (val_labels estara vacio).
# Es la fuente preferida para censos anteriores al 2022 o cuando no
# se dispone de los XLS del INDEC ni del motor completo.
# =============================================================================
construir_metadatos_dic <- function(dic_path) {

  dic <- redatam_open(dic_path)
  on.exit(try(redatam_close(dic), silent = TRUE))

  entidades <- tryCatch(
    redatam_entities(dic)$name,
    error = function(e) character(0)
  )

  if (length(entidades) == 0) {
    message("[ERROR] No se pudo leer la jerarquia del diccionario.")
    return(NULL)
  }

  var_labels <- list()

  for (entidad in entidades) {
    vars <- tryCatch(
      redatam_variables(dic, entidad),
      error = function(e) NULL
    )
    if (is.null(vars) || nrow(vars) == 0) next

    for (i in seq_len(nrow(vars))) {
      nom      <- toupper(vars$name[i])
      etiqueta <- if (!is.na(vars$label[i]) && nchar(trimws(vars$label[i])) > 0)
        vars$label[i] else nom
      var_labels[[nom]] <- etiqueta
    }
  }

  message("[INFO]  Metadatos desde diccionario: ",
          length(var_labels), " variables (sin etiquetas de valores)")

  # val_labels vacio — redatam_variables() no devuelve categorias
  list(var_labels = var_labels, val_labels = list())
}

# =============================================================================
# FUNCION INTERNA: construir_metadatos_xls()
#
# Alternativa preferida para obtener metadatos: lee los archivos XLS
# publicados por el INDEC con el diccionario de variables del censo.
# Es significativamente mas rapida que la fuente REDATAM y no consume
# RAM adicional del motor.
#
# Estructura del XLS del INDEC:
#   - Filas de variable: columna 1 con numero decimal (ej: "2.1", "3.4"),
#     columna 2 con el nombre de la variable, columna 3 con su rotulo.
#   - Filas de seccion "Categorias": marca el inicio de los valores.
#   - Filas de categoria: columna 3 con el codigo numerico,
#     columna 4 con la etiqueta de esa categoria.
# =============================================================================
construir_metadatos_xls <- function(xls_path) {

  df <- readxl::read_excel(xls_path, sheet = 1, col_names = FALSE)
  df <- as.data.frame(df, stringsAsFactors = FALSE)

  var_labels <- list()
  val_labels <- list()

  var_actual  <- NULL
  en_cats     <- FALSE
  cats_val    <- c()
  cats_lab    <- c()

  for (i in seq_len(nrow(df))) {
    num  <- df[i, 1]  # numero de variable (ej: "2.1")
    var  <- df[i, 2]  # nombre de la variable
    rot  <- df[i, 3]  # rotulo o valor de categoria
    tipo <- df[i, 4]  # tipo o etiqueta de categoria

    # Detectar fila de variable: el numero tiene formato decimal (ej: "2.1")
    num_str <- as.character(num)
    es_var  <- !is.na(num_str) && grepl("^[0-9]+\\.[0-9]+$", num_str) &&
      !is.na(var) && nchar(trimws(as.character(var))) > 0

    if (es_var) {
      # Cerrar la variable anterior guardando sus categorias acumuladas
      if (!is.null(var_actual) && length(cats_val) > 0) {
        val_labels[[var_actual]] <- data.frame(
          value = as.integer(cats_val),
          label = as.character(cats_lab),
          stringsAsFactors = FALSE
        )
      }
      # Registrar la nueva variable
      var_actual <- toupper(trimws(as.character(var)))
      etiqueta   <- if (!is.na(rot)) trimws(as.character(rot)) else var_actual
      var_labels[[var_actual]] <- etiqueta
      en_cats  <- FALSE
      cats_val <- c()
      cats_lab <- c()
      next
    }

    # Detectar la fila de encabezado "Categorias"
    if (!is.na(rot) && trimws(as.character(rot)) == "Categorias") {
      en_cats <- TRUE
      next
    }

    # Leer fila de categoria: codigo en rot, etiqueta en tipo
    if (en_cats && is.na(num) && is.na(var) && !is.na(rot) && !is.na(tipo)) {
      val_posible <- suppressWarnings(as.integer(rot))
      if (!is.na(val_posible)) {
        cats_val <- c(cats_val, val_posible)
        cats_lab <- c(cats_lab, trimws(as.character(tipo)))
      }
    }
  }

  # Cerrar la ultima variable del archivo
  if (!is.null(var_actual) && length(cats_val) > 0) {
    val_labels[[var_actual]] <- data.frame(
      value = as.integer(cats_val),
      label = as.character(cats_lab),
      stringsAsFactors = FALSE
    )
  }

  message("[INFO]  XLS procesado:", length(var_labels), "variables,",
      length(val_labels), "con categorias")

  list(var_labels = var_labels, val_labels = val_labels)
}


# =============================================================================
# FUNCION INTERNA: limpiar_nombre()
#
# El motor REDATAM agrega sufijos numericos a todos los nombres de columna
# en el output (ej: p01_0, edad_2, po_p22_3). Esta funcion los elimina y
# estandariza los nombres a mayusculas.
#
# Transformaciones aplicadas:
#   po_p22_3  ->  P22   (elimina prefijo "po_" de variables de Base PO)
#   p01_0     ->  P01   (elimina sufijo numerico)
#   idprov_4  ->  IDPROV
# =============================================================================
limpiar_nombre <- function(x) {
  x <- gsub("^po_", "", x, ignore.case = TRUE)  # prefijo de Base PO
  toupper(gsub("_[0-9]+$", "", x))               # sufijo numerico del motor
}

# =============================================================================
# FUNCION PRINCIPAL: censo_etiquetar()
# =============================================================================

#' Aplicar etiquetas a los microdatos extraidos del Censo
#'
#' @description
#' Aplica las etiquetas del diccionario a los archivos de microdatos
#' generados por \code{extraer_redatam()} o \code{extraer_dic()}. El proceso
#' realiza tres transformaciones sobre cada archivo:
#'
#' \enumerate{
#'   \item Limpia los nombres de columna eliminando sufijos numericos
#'     del motor REDATAM (\code{p01_0} -> \code{P01}).
#'   \item Agrega una etiqueta descriptiva a cada variable
#'     (atributo \code{label}).
#'   \item Convierte las variables categoricas a factor con las etiquetas
#'     de sus categorias (solo con fuente \code{"xls"} o \code{"redatam"}).
#' }
#'
#' Las variables geograficas identificadoras no se convierten a factor.
#'
#' El proceso es idempotente: si un archivo ya fue etiquetado, se omite.
#'
#' @param fuente_meta Character. Fuente de metadatos:
#'   \code{"auto"} (default): selecciona automaticamente la mejor fuente
#'   disponible en orden \code{xls > redatam > dic}.
#'   \code{"xls"}: XLS del INDEC (censo 2022, mejor calidad).
#'   \code{"redatam"}: motor REDATAM via subproceso (censo 2022 sin XLS).
#'   \code{"dic"}: diccionario directo via \code{redatam_variables()},
#'   sin etiquetas de valores. Recomendado para censos anteriores.
#' @param dic_path Character. Ruta al diccionario (.dicX o .rxdb).
#'   Requerido cuando \code{fuente_meta = "dic"} con un censo no 2022.
#'   Si es \code{NULL} (default), usa la base VP configurada.
#' @param provincias Numerico o \code{"all"}. Codigos de provincia.
#'   Default \code{"all"}.
#' @param bases Character o \code{"all"}. Bases a etiquetar.
#'   Default \code{"all"}.
#' @param etiquetar Character. Que etiquetas aplicar:
#'   \code{"todo"} (default), \code{"variables"}, \code{"valores"}.
#'
#' @return Invisible \code{NULL}.
#'
#' @examples
#' \dontrun{
#' # Seleccion automatica de fuente (recomendado)
#' censo_etiquetar()
#'
#' # Solo Formosa y Salta
#' censo_etiquetar(provincias = c(34, 66))
#'
#' # Censos anteriores con base .dicX
#' censo_etiquetar(
#'   fuente_meta = "dic",
#'   dic_path    = "D:/Censos/censo2010.dicX"
#' )
#' }
#'
#' @seealso \code{\link{extraer_redatam}}, \code{\link{extraer_dic}},
#'   \code{\link{censo_leer}}
#' @export
censo_etiquetar <- function(
    fuente_meta  = "auto",
    dic_path     = NULL,
    provincias   = "all",
    bases        = "all",
    etiquetar    = "todo"
) {

  stopifnot(etiquetar %in% c("todo", "variables", "valores"))
  fuente_meta <- match.arg(fuente_meta, c("auto", "xls", "redatam", "dic"))

  # Resolver rutas
  path        <- censo_dir_provincias()
  xls_path_vp <- censo_xls_vp()
  xls_path_vc <- censo_xls_vc()
  dic_path_vp <- if (!is.null(dic_path)) dic_path else censo_rxdb_vp()
  dic_path_vc <- censo_rxdb_vc()

  # ---- Seleccion automatica de fuente --------------------------------------
  if (fuente_meta == "auto") {
    if (file.exists(xls_path_vp)) {
      fuente_meta <- "xls"
      message("[INFO]  Fuente seleccionada automaticamente: xls")
    } else if (file.exists(dic_path_vp)) {
      fuente_meta <- "redatam"
      message("[INFO]  XLS no disponibles. Fuente seleccionada: redatam")
    } else {
      fuente_meta <- "dic"
      message("[INFO]  XLS y base VP no disponibles. Fuente seleccionada: dic")
    }
  }

  # ---- Cargar metadatos segun la fuente ------------------------------------
  meta_vp <- NULL
  meta_vc <- NULL

  if (fuente_meta == "xls") {

    if (file.exists(xls_path_vp)) {
      message("[INFO]  Cargando metadatos VP desde XLS...")
      meta_vp <- construir_metadatos_xls(xls_path_vp)
    } else {
      message("[AVISO] Diccionario VP (XLS) no encontrado.")
      message("[INFO]  Ejecute censo_descomprimir() si tiene el ZIP de metadatos,")
      message("[INFO]  o use fuente_meta = 'redatam' para etiquetar desde las bases.")
    }

    if (file.exists(xls_path_vc)) {
      message("[INFO]  Cargando metadatos VC desde XLS...")
      meta_vc <- construir_metadatos_xls(xls_path_vc)
    } else {
      message("[AVISO] Diccionario VC (XLS) no encontrado.")
      message("[INFO]  Ejecute censo_descomprimir() si tiene el ZIP de metadatos,")
      message("[INFO]  o use fuente_meta = 'redatam' para etiquetar desde las bases.")
    }

  } else if (fuente_meta == "redatam") {

    if (file.exists(dic_path_vp)) {
      message("[INFO]  Extrayendo metadatos VP desde REDATAM (puede demorar)...")
      tmp_meta_vp <- tempfile(fileext = ".rds")
      on.exit(unlink(tmp_meta_vp), add = TRUE)
      meta_vp <- construir_metadatos_subprocess(dic_path_vp, tmp_meta_vp)
      if (!is.null(meta_vp))
        message("[INFO]  VP: ", length(meta_vp$var_labels), " variables, ",
                length(meta_vp$val_labels), " con categorias")
    } else {
      message("[AVISO] Base VP no encontrada. Ejecute censo_descomprimir().")
    }

    if (file.exists(dic_path_vc)) {
      message("[INFO]  Extrayendo metadatos VC desde REDATAM (puede demorar)...")
      tmp_meta_vc <- tempfile(fileext = ".rds")
      on.exit(unlink(tmp_meta_vc), add = TRUE)
      meta_vc <- construir_metadatos_subprocess(dic_path_vc, tmp_meta_vc)
      if (!is.null(meta_vc))
        message("[INFO]  VC: ", length(meta_vc$var_labels), " variables, ",
                length(meta_vc$val_labels), " con categorias")
    } else {
      message("[AVISO] Base VC no encontrada.")
    }

  } else {  # fuente_meta == "dic"

    if (!is.null(dic_path) && file.exists(dic_path)) {
      message("[INFO]  Extrayendo metadatos desde diccionario: ", dic_path)
      meta_vp <- construir_metadatos_dic(dic_path)
      meta_vc <- meta_vp  # mismo diccionario para todas las entidades
    } else if (file.exists(dic_path_vp)) {
      message("[INFO]  Extrayendo metadatos VP desde diccionario configurado...")
      meta_vp <- construir_metadatos_dic(dic_path_vp)
      if (file.exists(dic_path_vc))
        meta_vc <- construir_metadatos_dic(dic_path_vc)
    } else {
      message("[AVISO] No se encontro ningun diccionario para extraer metadatos.")
    }

    if (!is.null(meta_vp))
      message("[INFO]  Nota: fuente 'dic' no incluye etiquetas de valores (categorias).")
  }

  if (is.null(meta_vp) && is.null(meta_vc))
    stop("No se pudieron obtener metadatos. Verifique la configuracion con censo_info().")

  gc()

  # ---- Obtener lista de archivos a procesar --------------------------------
  archivos <- list.files(path, pattern = "\\.(csv|sav|dta|parquet)$",
                         recursive = TRUE, full.names = TRUE,
                         ignore.case = TRUE)

  # Excluir archivos de verificacion, logs y codebooks
  archivos <- archivos[!grepl("codebook|verificacion|_log",
                              basename(archivos), ignore.case = TRUE)]

  # Filtrar por base — patron ampliado para cubrir extraer_redatam() y extraer_dic()
  if (!identical(bases, "all")) {
    # Mapeo de nombres amigables a patrones
    mapeo_bases <- c(
      "Personas"   = "Personas|PERSONA",
      "Hogares"    = "Hogares|HOGAR",
      "Viviendas"  = "Viviendas|VIVIENDA",
      "colectivas" = "colectivas",
      "PO_VP"      = "PO_VP"
    )
    patrones <- unname(mapeo_bases[bases])
    patrones <- patrones[!is.na(patrones)]
    if (length(patrones) > 0) {
      patron_bases <- paste(patrones, collapse = "|")
      archivos <- archivos[grepl(patron_bases, basename(archivos),
                                 ignore.case = TRUE)]
    }
  }

  # Filtrar por provincia
  if (!identical(provincias, "all")) {
    patron_prov <- paste0("[/\\\\](", paste(sprintf("%02d", provincias),
                                            collapse = "|"), ")_")
    archivos <- archivos[grepl(patron_prov, archivos)]
  }

  if (length(archivos) == 0) {
    message("[AVISO] No se encontraron archivos para etiquetar.")
    message("[INFO]  Verifique que los microdatos fueron extraidos.")
    return(invisible(NULL))
  }

  message("[INFO]  Archivos a etiquetar: ", length(archivos))
  n_ok      <- 0L
  n_omitido <- 0L

  # ---- Procesar cada archivo -----------------------------------------------
  for (arch in archivos) {
    message("[INFO]  Etiquetando: ", basename(arch))
    ext <- tolower(sub(".*\\.", "", basename(arch)))

    # Viviendas colectivas usan meta_vc; el resto usa meta_vp
    es_colectiva <- grepl("colectiv", basename(arch), ignore.case = TRUE)
    meta <- if (es_colectiva && !is.null(meta_vc)) meta_vc else meta_vp

    if (is.null(meta)) {
      message("[AVISO]  Sin metadatos para: ", basename(arch), " - omitido")
      n_omitido <- n_omitido + 1L
      next
    }

    tryCatch({

      # Leer el archivo
      df <- NULL

      if (ext == "parquet") {
        df <- as.data.frame(arrow::read_parquet(arch))
      } else if (ext == "csv") {
        df <- as.data.frame(data.table::fread(arch))
      } else if (ext == "sav") {
        df <- as.data.frame(haven::read_sav(arch))
      } else if (ext == "dta") {
        df <- as.data.frame(haven::read_dta(arch))
      }

      if (is.null(df) || nrow(df) == 0) {
        message("[INFO]   Archivo vacio, omitiendo: ", basename(arch))
        next
      }

      # Detectar si ya fue etiquetado verificando atributo label
      # Este criterio funciona tanto para archivos de extraer_redatam()
      # como para extraer_dic() independientemente de los nombres de columna
      ya_etiquetado <- any(sapply(df, function(x) !is.null(attr(x, "label"))))

      if (ya_etiquetado) {
        message("[INFO]   Ya etiquetado, omitiendo: ", basename(arch))
        n_omitido <- n_omitido + 1L
        next
      }

      # Limpiar nombres de columna (eliminar sufijos numericos si los tiene)
      names(df) <- limpiar_nombre(names(df))

      # Variables geograficas que NO se convierten a factor
      vars_geo_id <- c("REDCODEN", "IDPROV", "IDPTO", "IDFRAC", "IDRADIO",
                       "CODGL", "CODLOC", "CODAGLO", "TIPO_RADIO", "CATGL", "URP")

      # Aplicar etiquetas
      for (j in seq_along(names(df))) {
        nom <- toupper(names(df)[j])

        # Etiqueta descriptiva de la variable
        if (etiquetar %in% c("todo", "variables")) {
          if (nom %in% names(meta$var_labels) &&
              !is.null(meta$var_labels[[nom]]) &&
              nchar(trimws(meta$var_labels[[nom]])) > 0) {
            attr(df[[j]], "label") <- meta$var_labels[[nom]]
          }
        }

        # Convertir a factor con etiquetas de categorias
        if (etiquetar %in% c("todo", "valores")) {
          if (nom %in% names(meta$val_labels) && !nom %in% vars_geo_id) {
            tbl     <- meta$val_labels[[nom]]
            df[[j]] <- factor(df[[j]],
                              levels = tbl$value,
                              labels = tbl$label)
          }
        }
      }

      # Guardar sobreescribiendo
      if (ext == "parquet") {
        arrow::write_parquet(df, arch, compression = "snappy")
      } else if (ext == "csv") {
        data.table::fwrite(df, arch)
      } else if (ext == "sav") {
        haven::write_sav(df, arch)
      } else if (ext == "dta") {
        haven::write_dta(df, arch)
      }

      rm(df); gc()
      n_ok <- n_ok + 1L
      message("[OK]    ", basename(arch))

    }, error = function(e) {
      message("[ERROR] ", basename(arch), ": ", conditionMessage(e))
      n_omitido <<- n_omitido + 1L
    })
  }

  # ---- Resumen final -------------------------------------------------------
  if (n_omitido == length(archivos)) {
    message("[AVISO] Ningun archivo fue etiquetado.")
    message("[INFO]  Verifique que los diccionarios estan disponibles con censo_info().")
  } else {
    message("[INFO]  Proceso completado - ", n_ok, " etiquetados, ",
            n_omitido, " omitidos")
  }

  invisible(NULL)
}
