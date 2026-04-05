//
//  red_execute.cpp -- Redatam query/run API
//
//  Copyright (C) 2024 Jaime Salvador
//
//  This file is part of Redatam package
//
//  Redatam package is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 2 of the License, or
//  (at your option) any later version.
//
//  Redatam package is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
//
//  You should have received a copy of the GNU General Public License
//  along with Redatam package.  If not, see <http://www.gnu.org/licenses/>.
//
//  --------------------------------------------------------------------
//  Modificaciones: Copyright (C) 2026  Rodrigo Javier Durán
//  Instituto de Investigaciones en Energía No Convencional (INENCO)
//  CONICET — Universidad Nacional de Salta
//
//  Modificaciones incorporadas:
//    - struct FilteredOutput: estructura de contexto que acumula los
//      datos filtrados durante la iteración fila a fila del motor.
//    - dataset_filtered_row_handler(): callback que evalúa el filtro
//      geográfico en cada fila durante la iteración, descartando los
//      registros que no corresponden a la provincia indicada. Evita
//      cargar los 44 millones de registros en memoria.
//    - redatam_query_filtered(): función registrada en R que ejecuta
//      una consulta TABLE VIEW filtrando por el valor de una variable
//      entera o de cadena. Llama a redc_session_close() al finalizar
//      para liberar la memoria de sesión del motor.
//  --------------------------------------------------------------------

#include "cpp11.hpp"

#include <R.h>
#include <Rinternals.h>
#include <R_ext/Utils.h>

#include <vector>

#include "redengine_c.h"

extern std::shared_ptr<RedatamAPI> API;

// --------------------------------------------------------------------
// Código original de Jaime Salvador — sin modificaciones
// --------------------------------------------------------------------

bool __sp_callback (void *userData, int timeExpected, int timeElapsed, int percent, void *params) {

  R_CheckUserInterrupt();

  return false;

};

void compiler_handler_callback(int type, int code, char* code_str, char* msg, int line_no, int col_no) {

  char buff[100];

  if(type==0) { //exception
    REprintf("%s\n", msg);
  }
  else if(type==1) { //error
    snprintf(buff, sizeof(buff), "%s: line %d:%d %s", code_str, line_no, col_no, msg);
    REprintf("%s\n", buff);
  }
  else { //warning
    snprintf(buff, sizeof(buff), "%s: line %d:%d %s", code_str, line_no, col_no, msg);
    Rf_warning("%s\n", buff);
  }

  R_FlushConsole();
}

void dataset_new_row_handler(int row, int size, int* types, void** data, void* user_data) {
  std::vector<SEXP>* sexpColumns = (std::vector<SEXP> *)user_data;

  int cols = sexpColumns->size();

  for(int j=0; j<cols; j++ ) {
    SEXP col = sexpColumns->at(j);

    if( data[j]==nullptr ) {
      if( types[j]==1 ) {
        INTEGER(col)[static_cast<int>(row)] = NA_INTEGER;
      }
      else if( types[j]==2 ) {
        REAL(col)[static_cast<int>(row)] = NA_REAL;
      }
      else if( types[j]==3 ) {
        SET_STRING_ELT(col,row, NA_STRING );
      }
    }
    else {
      if( types[j]==1 ) {
        int64_t* value = (int64_t *)data[j];
        INTEGER(col)[static_cast<int>(row)] = *value;
      }
      else if( types[j]==2 ) {
        double* value = (double *)data[j];
        REAL(col)[static_cast<int>(row)] = *value;
      }
      else if( types[j]==3 ) {
        char* value = (char *)data[j];
        SET_STRING_ELT(col,row, Rf_mkChar(value));
      }
    }
  }
}

SEXP createOutput( void* session_ptr, int index ) {
  int out_cols=2;
  int out_rows=2;
  int out_type=1;
  int dimension;

  std::vector<char*> out_name(10);

  API->redc_session_output_data(session_ptr, index, &out_type, &dimension, &out_cols, &out_rows, (char **)out_name.data());

  std::vector<int> fields_type(out_cols);
  std::vector<char*> fields_name(out_cols);

  API->redc_session_output_fields_type(session_ptr, index, fields_type.data(), (char **)fields_name.data());

  std::vector<SEXP> sexpColumns;
  for(int i=0;i<out_cols;i++) {
    SEXP col;

    if(fields_type[i]==1) { // integer
      Rf_protect(col=Rf_allocVector(INTSXP, out_rows));
    }
    else if(fields_type[i]==2) { // real
      Rf_protect(col=Rf_allocVector(REALSXP, out_rows));
    }
    else if(fields_type[i]==3) { // string
      Rf_protect(col=Rf_allocVector(STRSXP, out_rows));
    }

    sexpColumns.push_back(col);
  }

  API->redc_session_output_iterate(session_ptr, index, dataset_new_row_handler, (void *)&sexpColumns);

  SEXP ans = PROTECT(Rf_allocVector(VECSXP, out_cols));
  SEXP nms = PROTECT(Rf_allocVector(STRSXP, out_cols));
  SEXP rnms = PROTECT(Rf_allocVector(INTSXP, 2));

  for(int i=0; i<out_cols; i++ ) {
    SET_STRING_ELT(nms, i, Rf_mkCharCE( fields_name[i], CE_UTF8 ));
    SET_VECTOR_ELT(ans, i, sexpColumns[i] );
  }

  INTEGER(rnms)[0] = NA_INTEGER;
  INTEGER(rnms)[1] = -out_rows;

  Rf_setAttrib(ans, R_ClassSymbol, Rf_ScalarString(Rf_mkChar("data.frame")));
  Rf_setAttrib(ans, R_RowNamesSymbol, rnms);
  Rf_setAttrib(ans, R_NamesSymbol, nms);

  std::string tableType = "";
  std::string tableName = std::string(out_name[0]);

  if( out_type==0 ){
    tableType = "table";
  }
  else if( out_type==1 ){
    tableType = "arealist";
  }
  else if( out_type==2 ){
    tableType = "tablist";
  }
  else if( out_type==3 ){
    tableType = "tabop";
  }

  Rf_setAttrib(ans, Rf_mkString("redatam.table.type"), Rf_mkString(tableType.c_str()));
  Rf_setAttrib(ans, Rf_mkString("redatam.table.name"), Rf_mkString(tableName.c_str()));

  SEXP vars = Rf_allocVector( STRSXP, 1 );
  Rf_setAttrib(ans, Rf_mkString("redatam.table.vars"), vars);

  UNPROTECT(out_cols+3);

  return(ans);
}

[[cpp11::register]]
cpp11::writable::list redatam_internal_query( SEXP dic, const std::string& spc )
{
  void* ptr = R_ExternalPtrAddr(dic);

  if(ptr==nullptr) {
    cpp11::stop("Dictionary must be a valid object" );
  }

  if( spc.empty() ) {
    cpp11::stop("SPC can't be empty" );
  }

  void* session_ptr = API->redc_run_program(ptr, spc.c_str(), compiler_handler_callback,__sp_callback);

  if(session_ptr==nullptr) {
    return cpp11::writable::list();
  }

  int count = API->redc_session_output_count(session_ptr);

  if(count==0) {
    return cpp11::writable::list();
  }

  std::vector<SEXP> ret;

  for(int i=0;i<count;i++) {
    SEXP df = createOutput(session_ptr, i);
    ret.push_back(df);
  }

  return cpp11::writable::list(ret.begin(), ret.end());

}

[[cpp11::register]]
cpp11::writable::list redatam_internal_run( SEXP dic, const std::string& file_name ) {

  void* ptr = R_ExternalPtrAddr(dic);

  if(ptr==nullptr) {
    cpp11::stop("Dictionary must be a valid object" );
  }

  if( file_name.empty() ) {
    cpp11::stop("SPC file_name can't be empty" );
  }

  void* session_ptr = API->redc_run_program_file(ptr, file_name.c_str(), compiler_handler_callback, __sp_callback);

  if(session_ptr==nullptr) {
    return {};
  }

  int count = API->redc_session_output_count(session_ptr);

  if(count==0) {
    return {};
  }

  std::vector<SEXP> ret;

  for(int i=0;i<count;i++) {
    SEXP df = createOutput(session_ptr, i);
    ret.push_back(df);
  }

  return cpp11::writable::list(ret.begin(), ret.end());
}

// --------------------------------------------------------------------
// Código agregado por Rodrigo Javier Durán (INENCO/CONICET) — 2025
// --------------------------------------------------------------------

// Estructura de contexto para la extracción con filtro geográfico.
// Acumula los datos de las filas que pasan el filtro durante la
// iteración fila a fila del motor, separados por tipo de dato
// (entero, real, cadena) para construcción eficiente del data.frame.
struct FilteredOutput {
  std::vector<std::vector<int>>         int_cols;
  std::vector<std::vector<double>>      dbl_cols;
  std::vector<std::vector<std::string>> str_cols;
  std::vector<int> col_types;
  int filter_col_idx;  // índice de la columna usada como filtro (-1 = sin filtro)
  int filter_value;    // valor entero a comparar
  std::string filter_str;  // valor como cadena (para columnas de tipo string)
  int written;         // contador de filas que pasaron el filtro
};

// Callback invocado por redc_session_output_iterate para cada fila.
// Evalúa el filtro geográfico en cada fila durante la iteración y
// descarta las que no corresponden a la unidad geográfica indicada.
// Solo las filas que pasan el filtro se acumulan en FilteredOutput.
//
// El filtrado en el callback es la única forma de evitar cargar los
// 44 millones de registros del censo en RAM: el motor itera internamente
// y solo transfiere a R las filas seleccionadas.
void dataset_filtered_row_handler(int row, int size, int* types, void** data, void* user_data) {
  FilteredOutput* ctx = (FilteredOutput*)user_data;

  // Evaluar filtro: descartar fila si no coincide con el valor buscado
  if (ctx->filter_col_idx >= 0) {
    if (data[ctx->filter_col_idx] == nullptr) return;
    if (ctx->col_types[ctx->filter_col_idx] == 3) {
      // Columna de tipo string: comparar como cadena
      char* prov = (char*)data[ctx->filter_col_idx];
      if (ctx->filter_str != std::string(prov)) return;
    } else if (ctx->col_types[ctx->filter_col_idx] == 1) {
      // Columna de tipo entero: comparar numéricamente
      int64_t prov = *(int64_t*)data[ctx->filter_col_idx];
      if ((int)prov != ctx->filter_value) return;
    }
  }

  // Fila que pasó el filtro: acumular sus valores por tipo
  int int_i=0, dbl_i=0, str_i=0;
  for (int j = 0; j < size; j++) {
    if (ctx->col_types[j] == 1) {
      int64_t v = data[j] ? *(int64_t*)data[j] : (int64_t)NA_INTEGER;
      ctx->int_cols[int_i++].push_back((int)v);
    } else if (ctx->col_types[j] == 2) {
      double v = data[j] ? *(double*)data[j] : NA_REAL;
      ctx->dbl_cols[dbl_i++].push_back(v);
    } else if (ctx->col_types[j] == 3) {
      std::string v = data[j] ? (char*)data[j] : "";
      ctx->str_cols[str_i++].push_back(v);
    }
  }
  ctx->written++;
}

// Ejecuta una consulta TABLE VIEW filtrando los resultados por el valor
// de una variable durante la iteración del motor, sin cargar todo el
// dataset en memoria. Registrada en R como redatam_query_filtered().
//
// Parámetros:
//   dic          — diccionario REDATAM abierto
//   spc          — programa SPC con la consulta TABLE VIEW
//   filter_var   — nombre de la variable usada como filtro (ej: "IDPROV")
//   filter_value — valor entero a retener (ej: 34 para Formosa)
//
// La variable de filtro se busca por nombre en las columnas del output,
// con padding automático de 2 dígitos para columnas de tipo string.
// Al finalizar, libera la memoria de sesión del motor con redc_session_close.
[[cpp11::register]]
cpp11::writable::list redatam_query_filtered(
    SEXP dic,
    std::string spc,
    std::string filter_var,
    int filter_value)
{
  void* ptr = R_ExternalPtrAddr(dic);
  if(ptr==nullptr) return cpp11::writable::list();

  auto session_ptr = API->redc_run_program(ptr, spc.c_str(), compiler_handler_callback, __sp_callback);
  if (!session_ptr) return cpp11::writable::list();

  int count = API->redc_session_output_count(session_ptr);
  if (count == 0) return cpp11::writable::list();

  int out_cols=2, out_rows=2, out_type=1, dimension;
  std::vector<char*> out_name(10);
  API->redc_session_output_data(session_ptr, 0, &out_type, &dimension, &out_cols, &out_rows, (char**)out_name.data());

  std::vector<int> fields_type(out_cols);
  std::vector<char*> fields_name(out_cols);
  API->redc_session_output_fields_type(session_ptr, 0, fields_type.data(), (char**)fields_name.data());

  // Localizar la columna de filtro por nombre (sin sufijo numérico del motor)
  int filter_idx = -1;
  for (int i = 0; i < out_cols; i++) {
    if (fields_name[i]) {
      std::string fname(fields_name[i]);
      size_t pos = fname.find('_');
      std::string base = (pos != std::string::npos) ? fname.substr(0, pos) : fname;
      if (strcasecmp(base.c_str(), filter_var.c_str()) == 0) {
        filter_idx = i;
        break;
      }
    }
  }

  // Inicializar contexto de filtrado
  FilteredOutput ctx;
  ctx.filter_col_idx = filter_idx;
  ctx.filter_value   = filter_value;
  // Representación string con padding de 2 dígitos (ej: 2 → "02")
  ctx.filter_str = std::to_string(filter_value);
  if (ctx.filter_str.length() == 1) ctx.filter_str = "0" + ctx.filter_str;
  ctx.written   = 0;
  ctx.col_types = fields_type;

  // Reservar vectores por tipo de columna
  for (int i = 0; i < out_cols; i++) {
    if (fields_type[i] == 1)      ctx.int_cols.emplace_back();
    else if (fields_type[i] == 2) ctx.dbl_cols.emplace_back();
    else if (fields_type[i] == 3) ctx.str_cols.emplace_back();
  }

  // Iterar fila a fila: el filtrado ocurre en el callback
  API->redc_session_output_iterate(session_ptr, 0, dataset_filtered_row_handler, (void*)&ctx);

  // Construir el data.frame resultado con las filas filtradas
  int n = ctx.written;
  cpp11::writable::list result(out_cols);
  cpp11::writable::strings col_names(out_cols);

  int int_i=0, dbl_i=0, str_i=0;
  for (int i = 0; i < out_cols; i++) {
    col_names[i] = fields_name[i] ? fields_name[i] : "";
    if (fields_type[i] == 1) {
      cpp11::writable::integers col(ctx.int_cols[int_i].begin(), ctx.int_cols[int_i].end());
      result[i] = col; int_i++;
    } else if (fields_type[i] == 2) {
      cpp11::writable::doubles col(ctx.dbl_cols[dbl_i].begin(), ctx.dbl_cols[dbl_i].end());
      result[i] = col; dbl_i++;
    } else if (fields_type[i] == 3) {
      cpp11::writable::strings col(n);
      for (int k = 0; k < n; k++) col[k] = ctx.str_cols[str_i][k];
      result[i] = col; str_i++;
    }
  }

  result.names() = col_names;

  // Liberar memoria de sesión del motor C++.
  // Sin esta llamada, la RAM acumulada por el motor no es liberada
  // hasta que se destruye el proceso completo de R.
  API->redc_session_close(session_ptr);

  return result;
}
