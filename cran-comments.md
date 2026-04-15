## Resubmission (2nd)

This is a second resubmission addressing all points raised by Benjamin Altmann.

### Response to reviewer comments

**1. English translation of Description**
Added. The Description field is now in English. The Title field was also
translated to English for consistency.

**2. Missing \value tag in censo_descargar.Rd**
Added \value documenting that the function returns invisible NULL and is
called for its side effects.

**3. \dontrun{} replaced by \donttest{}**
Done for all functions where examples can in principle be executed:
censo_configurar(), censo_verificar_engine(), and censo_info().

Functions that require external census data files (~500 MB) downloaded
directly from INDEC (Argentina's national statistics office) under Law
17.622 on statistical secrecy retain \dontrun{}: censo_descargar(),
censo_etiquetar(), censo_leer(), extraer_redatam(), and extraer_rxdb().
These files cannot be redistributed or included in the package. This
matches the intended use of \dontrun{} per CRAN policy.

**4. print()/cat() replaced by message()**
All informational console output now uses message() throughout the package,
allowing suppression via suppressMessages(). Exceptions retained as cat():
- Interactive readline() prompts (message() breaks readline() behavior)
- Progress bars using \r (message() does not support carriage return)
- Script output captured via intern = TRUE in system() calls (message()
  writes to stderr, breaking capture)

**5. Writing to user home filespace**
censo_configurar() no longer has a default path. The dir argument is now
required. Examples use tempdir() for the executable example, with real
paths shown as comments only.

**6. Authors, contributors and copyright holders in Authors@R**
Added Jaime Salvador (ctb) as contributor for red_execute.cpp,
red_initialize.cpp and redengine_c.h, which are derived from the redatamx
package. Added CELADE (cph) as copyright holder of the REDATAM engine.

## R CMD check results

0 errors | 0 warnings | 1 note

* checking for future file timestamps: NOTE
  Unable to verify current time. This is a connectivity issue on the
  check server and is unrelated to the package.

## Test environments

* Fedora Linux 43, R 4.5.2
* Windows 10, R 4.5.2
* GitHub Actions: ubuntu-latest, R release (via r-lib/actions)

## Downstream dependencies

None.
