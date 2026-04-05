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
#   censo_etiquetar() - funcion principal
#
# Funciones internas:
#   construir_metadatos()            - extrae etiquetas del .rxdb
#   construir_metadatos_subprocess() - idem en subproceso (gestion de RAM)
#   construir_metadatos_xls()        - extrae etiquetas desde XLS del INDEC
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
        tbl <- tryCatch(
          redatam_query(dic, paste0("FREQ ", entidad, ".", vars$name[i]))[[1]],
          error = function(e) NULL
        )
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
        tbl <- tryCatch(
          redatam_query(dic, paste0("FREQ ", entidad, ".", vars$name[i]))[[1]],
          error = function(e) NULL
        )
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
  for (linea in output) cat("[INFO]  [meta]", linea, "\n")

  ret <- attr(output, "status")
  ret <- if (is.null(ret)) 0L else ret

  if (ret != 0 || !file.exists(out_file)) {
    cat("[ERROR] No se pudieron construir los metadatos desde:", dic_path, "\n")
    return(NULL)
  }

  meta <- readRDS(out_file)
  file.remove(out_file)
  meta
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

  cat("[INFO]  XLS procesado:", length(var_labels), "variables,",
      length(val_labels), "con categorias\n")

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

#' Aplicar etiquetas a los microdatos extraidos del Censo 2022
#'
#' @description
#' Aplica las etiquetas del diccionario oficial del INDEC a los archivos
#' de microdatos generados por \code{extraer_redatam()}. El proceso
#' realiza tres transformaciones sobre cada archivo:
#'
#' \enumerate{
#'   \item Limpia los nombres de columna eliminando los sufijos numericos
#'     del motor REDATAM (\code{p01_0} -> \code{P01}).
#'   \item Agrega una etiqueta descriptiva a cada variable
#'     (atributo \code{label}).
#'   \item Convierte las variables categoricas a factor con las etiquetas
#'     de sus categorias (ej: \code{1} -> \code{"Jefa o jefe"}).
#' }
#'
#' Las variables geograficas identificadoras no se convierten a factor
#' porque sus codigos no corresponden al nivel jerarquico del diccionario.
#'
#' El proceso es idempotente: si un archivo ya fue etiquetado, se detecta
#' automaticamente y se omite sin modificarlo.
#'
#' @param fuente_meta Character. Fuente de los metadatos de etiquetado.
#'   \code{"xls"} (default): usa los archivos XLS del INDEC, mas rapido
#'   y sin consumo adicional de memoria. \code{"redatam"}: extrae las
#'   etiquetas directamente del motor REDATAM via subproceso, util cuando
#'   no se dispone de los archivos XLS.
#' @param provincias Numerico o \code{"all"}. Codigos de provincia a
#'   etiquetar. Default \code{"all"}.
#' @param bases Character o \code{"all"}. Bases a etiquetar:
#'   \code{"Personas"}, \code{"Hogares"}, \code{"Viviendas"},
#'   \code{"colectivas"}, \code{"PO_VP"}. Default \code{"all"}.
#' @param etiquetar Character. Que etiquetas aplicar:
#'   \code{"todo"} (default), \code{"variables"} (solo nombres),
#'   \code{"valores"} (solo categorias).
#'
#' @return Invisible \code{NULL}. Modifica los archivos en el directorio
#'   configurado.
#'
#' @examples
#' \dontrun{
#' # Etiquetar todas las provincias (configuracion automatica)
#' censo_etiquetar()
#'
#' # Solo Formosa y Salta
#' censo_etiquetar(provincias = c(34, 66))
#'
#' # Solo la base de Personas
#' censo_etiquetar(bases = "Personas")
#'
#' # Usando el motor REDATAM como fuente de metadatos
#' censo_etiquetar(fuente_meta = "redatam")
#' }
#'
#' @seealso \code{\link{extraer_redatam}}, \code{\link{censo_leer}}
#' @export
censo_etiquetar <- function(
    fuente_meta  = "xls",
    provincias   = "all",
    bases        = "all",
    etiquetar    = "todo"
) {

  stopifnot(etiquetar %in% c("todo", "variables", "valores"))
  fuente_meta <- match.arg(fuente_meta, c("xls", "redatam"))

  # Resolver rutas desde la configuracion del paquete
  path        <- censo_dir_provincias()
  xls_path_vp <- censo_xls_vp()
  xls_path_vc <- censo_xls_vc()
  dic_path_vp <- censo_rxdb_vp()
  dic_path_vc <- censo_rxdb_vc()

  # ---- Cargar metadatos segun la fuente elegida ----------------------------
  meta_vp <- NULL
  meta_vc <- NULL

  if (fuente_meta == "xls") {

    # Fuente XLS: rapida y sin costo de RAM adicional
    if (file.exists(xls_path_vp)) {
      cat("[INFO] Cargando metadatos VP desde XLS...\n")
      meta_vp <- construir_metadatos_xls(xls_path_vp)
    } else {
      cat("[AVISO] Diccionario VP (XLS) no encontrado. Ejecute censo_descargar().\n")
    }

    if (file.exists(xls_path_vc)) {
      cat("[INFO] Cargando metadatos VC desde XLS...\n")
      meta_vc <- construir_metadatos_xls(xls_path_vc)
    } else {
      cat("[AVISO] Diccionario VC (XLS) no encontrado. Ejecute censo_descargar().\n")
    }

  } else {

    # Fuente REDATAM: extrae etiquetas del motor en subproceso
    if (file.exists(dic_path_vp)) {
      cat("[INFO] Extrayendo metadatos VP desde REDATAM (puede demorar)...\n")
      tmp_meta_vp <- tempfile(fileext = ".rds")
      meta_vp <- construir_metadatos_subprocess(dic_path_vp, tmp_meta_vp)
      if (!is.null(meta_vp))
        cat("[INFO] VP:", length(meta_vp$var_labels), "variables,",
            length(meta_vp$val_labels), "con categorias\n")
    } else {
      cat("[AVISO] Base VP no encontrada. Ejecute censo_descargar().\n")
    }

    if (file.exists(dic_path_vc)) {
      cat("[INFO] Extrayendo metadatos VC desde REDATAM (puede demorar)...\n")
      tmp_meta_vc <- tempfile(fileext = ".rds")
      meta_vc <- construir_metadatos_subprocess(dic_path_vc, tmp_meta_vc)
      if (!is.null(meta_vc))
        cat("[INFO] VC:", length(meta_vc$var_labels), "variables,",
            length(meta_vc$val_labels), "con categorias\n")
    } else {
      cat("[AVISO] Base VC no encontrada. Ejecute censo_descargar().\n")
    }
  }

  if (is.null(meta_vp) && is.null(meta_vc)) {
    stop("No se pudieron obtener metadatos. Verifique la configuracion con censo_info().")
  }

  gc()

  # ---- Obtener lista de archivos a procesar --------------------------------

  archivos <- list.files(path, pattern = "\\.(csv|sav|dta|parquet)$",
                         recursive = TRUE, full.names = TRUE,
                         ignore.case = TRUE)

  # Excluir archivos de verificacion, logs y codebooks
  archivos <- archivos[!grepl("codebook|verificacion|_log",
                              basename(archivos), ignore.case = TRUE)]

  # Filtrar por base si se especifico
  if (!identical(bases, "all")) {
    patron_bases <- paste(bases, collapse = "|")
    archivos <- archivos[grepl(patron_bases, basename(archivos), ignore.case = TRUE)]
  }

  # Filtrar por provincia si se especifico
  if (!identical(provincias, "all")) {
    patron_prov <- paste0("/(", paste(sprintf("%02d", provincias), collapse = "|"), ")_")
    archivos <- archivos[grepl(patron_prov, archivos)]
  }

  if (length(archivos) == 0) {
    cat("[AVISO] No se encontraron archivos para etiquetar.\n")
    cat("[INFO]  Verifique que los microdatos fueron extraidos con extraer_redatam().\n")
    return(invisible(NULL))
  }

  cat("[INFO] Archivos a etiquetar:", length(archivos), "\n")
  n_ok      <- 0L
  n_omitido <- 0L

  # ---- Procesar cada archivo -----------------------------------------------
  for (arch in archivos) {
    cat("[INFO] Etiquetando:", basename(arch), "\n")
    ext <- tolower(tools::file_ext(arch))

    # Las viviendas colectivas usan el diccionario VC; el resto usa VP
    es_colectiva <- grepl("colectiv", basename(arch), ignore.case = TRUE)
    meta <- if (es_colectiva && !is.null(meta_vc)) meta_vc else meta_vp

    if (is.null(meta)) {
      cat("[AVISO]  Sin metadatos para:", basename(arch), "- omitido\n")
      n_omitido <- n_omitido + 1L
      next
    }

    tryCatch({

      # Detectar si el archivo ya fue etiquetado
      # Los archivos sin etiquetar tienen sufijos numericos en los nombres (ej: p01_0)
      ya_etiquetado <- if (ext == "parquet") {
        noms <- names(arrow::read_parquet(arch, as_data_frame = FALSE)$schema)
        !any(grepl("_[0-9]+$", noms))
      } else if (ext == "csv") {
        df_test <- data.table::fread(arch, nrows = 1)
        res <- !any(grepl("_[0-9]+$", names(df_test)))
        rm(df_test); res
      } else {
        FALSE  # formatos sav/dta: siempre procesar
      }

      if (ya_etiquetado) {
        cat("[INFO]  Ya etiquetado, omitiendo:", basename(arch), "\n")
        next
      }

      # Leer el archivo segun su formato
      df <- NULL
      on.exit({
        if (exists("df") && !is.null(df)) { rm(df); gc() }
      }, add = TRUE)

      if (ext == "parquet") {
        df <- as.data.frame(arrow::read_parquet(arch))
      } else if (ext == "csv") {
        df <- as.data.frame(data.table::fread(arch, skip = "auto"))
      } else if (ext == "sav") {
        df <- as.data.frame(haven::read_sav(arch))
      } else if (ext == "dta") {
        df <- as.data.frame(haven::read_dta(arch))
      }

      if (is.null(df) || nrow(df) == 0) {
        cat("[INFO]  Archivo vacio, omitiendo:", basename(arch), "\n")
        next
      }

      # Limpiar nombres de columna (eliminar sufijos numericos del motor REDATAM)
      names(df) <- limpiar_nombre(names(df))

      # Variables geograficas que NO deben convertirse a factor.
      # Sus codigos son identificadores compuestos (ej: REDCODEN = 340070101)
      # que no corresponden a las categorias del diccionario de departamentos.
      vars_geo_id <- c("REDCODEN", "IDPROV", "IDPTO", "IDFRAC", "IDRADIO",
                       "CODGL", "CODLOC", "CODAGLO", "TIPO_RADIO", "CATGL", "URP")

      # Aplicar etiquetas variable por variable
      for (j in seq_along(names(df))) {
        nom <- names(df)[j]

        # Etiqueta descriptiva de la variable
        if (etiquetar %in% c("todo", "variables")) {
          if (nom %in% names(meta$var_labels)) {
            attr(df[[j]], "label") <- meta$var_labels[[nom]]
          }
        }

        # Convertir a factor con etiquetas de categorias
        # Se excluyen las variables geograficas identificadoras
        if (etiquetar %in% c("todo", "valores")) {
          if (nom %in% names(meta$val_labels) && !nom %in% vars_geo_id) {
            tbl     <- meta$val_labels[[nom]]
            df[[j]] <- factor(df[[j]],
                              levels = tbl$value,
                              labels = tbl$label)
          }
        }
      }

      # Guardar sobreescribiendo el archivo original
      if (ext == "parquet") {
        arrow::write_parquet(df, arch, compression = "snappy")
      } else if (ext == "csv") {
        data.table::fwrite(df, arch)
      } else if (ext == "sav") {
        haven::write_sav(df, arch)
      } else if (ext == "dta") {
        haven::write_dta(df, arch)
      }

      n_ok <- n_ok + 1L
      cat("[INFO]  OK:", basename(arch), "\n")

    }, error = function(e) {
      cat("[ERROR]", basename(arch), ":", conditionMessage(e), "\n")
    })
  }

  # ---- Resumen final -------------------------------------------------------
  if (n_omitido == length(archivos)) {
    cat("[AVISO] Ningun archivo fue etiquetado.\n")
    cat("[INFO]  Verifique que los diccionarios estan disponibles con censo_info().\n")
  } else if (n_omitido > 0) {
    cat("[INFO] Proceso completado -", n_ok, "etiquetados,", n_omitido, "omitidos\n")
  } else {
    cat("[INFO] Proceso completado -", n_ok, "archivo(s) etiquetado(s)\n")
  }

  invisible(NULL)
}
