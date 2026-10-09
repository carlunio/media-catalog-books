# Configuración local

## Primer arranque

La instalación no requiere preparar `.env` manualmente. Al ejecutar
`tools/set-up-app.*`, `tools/launch-app.*` o el comando equivalente, `appctl`
copia `.env.example` a `.env` si todavía no existe. Una configuración ya
existente nunca se sobrescribe.

Los valores iniciales permiten abrir la aplicación sin claves externas. OCR y
catalogación usan Ollama; OpenAI e ISBNdb se habilitan al añadir sus claves.
Antes de trabajar con esos flujos debe estar iniciado Ollama y deben estar
instalados los modelos configurados.

## Grupos de variables

- Aplicación: `PROJECT_ROOT`, `APP_CHANNEL` y `APP_ICON_URL`.
- Actualización: `GIT_REMOTE`, `GIT_BRANCH`, tiempos de espera y conservación
  de copias de seguridad.
- Servicios: `BACK_HOST`, `BACK_PORT`, `FRONT_HOST`, `FRONT_PORT` y `API_URL`.
- Datos: rutas de DuckDB, entradas, portadas, exportaciones y OCR.
- Instantáneas: `BBDD_DIR`, estado, identidad del equipo y conservación.
- Proveedores: modelos y selección de Ollama/OpenAI.
- Integraciones opcionales: `OPENAI_API_KEY`, `GOOGLE_BOOKS_API_KEY`,
  `ISBNDB_API_KEY` y `OPENLIBRARY_CONTACT`.

`API_URL` puede quedar vacío: `appctl` lo deriva de `BACK_PORT`. `SYNC_ACTOR` y
`SYNC_DEVICE` también pueden quedar vacíos para usar la identidad del sistema.
Las rutas relativas se resuelven desde la raíz del repositorio.

`GOOGLE_BOOKS_API_KEY` identifica el proyecto ante Google Books y evita
depender de la cuota anónima compartida. `OPENLIBRARY_CONTACT` debe contener
un correo de contacto para identificar la aplicación en el
`User-Agent` enviado a Open Library. Los intervalos predeterminados de Google
Books y Open Library son de un segundo; pueden aumentarse mediante
`GOOGLE_BOOKS_MIN_INTERVAL_SECONDS` y `OPENLIBRARY_MIN_INTERVAL_SECONDS`.

`OCR_OLLAMA_MODEL_SUGGESTIONS` y `CATALOG_OLLAMA_MODEL_SUGGESTIONS`
son listas separadas por comas que alimentan los selectores de la interfaz.
`CATALOG_MODEL` se conserva por compatibilidad y sirve como respaldo de
`CATALOG_OPENAI_MODEL` cuando esta variable no se define.

Los lotes del flujo de trabajo no tienen un tiempo de espera HTTP global. Cada
operación externa
tiene su propio límite, que se reinicia en cada llamada y en cada libro:

- `REQUEST_TIMEOUT_SECONDS` limita cada petición a Google Books, Open Library,
  ISBNdb y la descarga de una portada (20 segundos por defecto).
- `LLM_TIMEOUT_SECONDS` limita cada llamada de OCR o catalogación (300 segundos
  por defecto).
- `OLLAMA_TIMEOUT_SECONDS` permite usar otro límite solo para Ollama; si queda
  vacío, hereda `LLM_TIMEOUT_SECONDS`.

Si una llamada agota su tiempo, ese libro sigue la ruta normal de reintento y
revisión. El lote continúa con el siguiente libro cuando termina esa ruta.

## Configurar Google Books

### Requisitos

Para consultar datos públicos de libros se necesita una cuenta de Google, un
proyecto de Google Cloud y una clave estándar de API. La aplicación no accede
a la biblioteca privada de ningún usuario, por lo que no necesita OAuth, un
cliente OAuth, una cuenta de servicio ni una pantalla de consentimiento.

- Si hace falta una cuenta, créala en
  [Crear una cuenta de Google](https://accounts.google.com/signup).
- La guía oficial de Books API explica la identificación de peticiones en
  [Using the API](https://developers.google.com/books/docs/v1/using).
- Google documenta la creación y las restricciones en
  [Gestionar claves de API](https://docs.cloud.google.com/docs/authentication/api-keys).

### Crear el proyecto y habilitar Books API

1. Abre [Crear un proyecto de Google Cloud](https://console.cloud.google.com/projectcreate).
2. Escribe un nombre reconocible, por ejemplo `media-catalog-books`, y crea el
   proyecto. Si la cuenta pertenece a una organización, selecciona la
   organización y la ubicación que correspondan.
3. Cuando termine, usa el selector de la barra superior para dejar activo ese
   proyecto. Los pasos siguientes deben hacerse siempre dentro del mismo
   proyecto.
4. Abre la ficha directa de
   [Books API](https://console.cloud.google.com/apis/api/books.googleapis.com/overview)
   y pulsa **Habilitar**. Si aparece **Gestionar**, ya está habilitada.

El nombre que debe aparecer es **Books API** y su identificador de servicio es
`books.googleapis.com`. No hay que buscar una opción llamada «eBooks API».

### Crear y restringir la clave

1. Sin cambiar de proyecto, abre
   [APIs y servicios > Credenciales](https://console.cloud.google.com/apis/credentials).
2. Pulsa **Crear credenciales** y elige **Clave de API**. Crea una clave
   estándar; no la vincules a una cuenta de servicio.
3. Ponle un nombre descriptivo, por ejemplo `media-catalog-books-local`.
4. En **Restricciones de API**, elige **Restringir clave** y selecciona
   únicamente **Books API**.
5. En **Restricciones de aplicación**:
   - si el equipo sale siempre por una IP pública fija, se puede elegir
     **Direcciones IP** y registrar esa IP;
   - si la conexión doméstica usa una IP dinámica, deja esta restricción sin
     configurar y conserva al menos la restricción a **Books API**;
   - no elijas **Sitios web** o **Referentes HTTP**: las peticiones salen del
     backend de Python y no del navegador.
6. Guarda los cambios y copia la **cadena de la clave**. El identificador
   interno de la clave que aparece en la URL de la consola no sirve para hacer
   peticiones.

Google recomienda combinar restricciones de API y de aplicación. En un equipo
doméstico con IP dinámica, una restricción por IP puede dejar de funcionar al
cambiar la dirección; por eso la restricción imprescindible para este caso es
que la clave solo pueda llamar a **Books API**.

Si **Books API** no aparece en la lista de restricciones:

1. comprueba en el selector superior que la clave y la API están en el mismo
   proyecto;
2. vuelve a la ficha de Books API y verifica que muestra **Gestionar**;
3. espera unos minutos y recarga la página de credenciales;
4. si sigue sin aparecer, crea una clave nueva después de habilitar la API.

### Configurar `.env`

Abre el `.env` local y pega la cadena completa sin comillas ni espacios:

```dotenv
GOOGLE_BOOKS_API_KEY=AIza...clave_completa...
GOOGLE_BOOKS_MIN_INTERVAL_SECONDS=1
REQUEST_TIMEOUT_SECONDS=20
```

`GOOGLE_BOOKS_MIN_INTERVAL_SECONDS` fija la separación mínima entre peticiones
de Google. `REQUEST_TIMEOUT_SECONDS` es el límite de cada petición
bibliográfica, no un límite para el lote completo. El valor real de la clave
debe quedarse únicamente en `.env`, que está excluido de Git; no debe copiarse
a `.env.example`, capturas, incidencias, documentación o commits.

Reinicia la aplicación para cargar la variable:

```bash
make stop
make start
```

El cliente consulta `https://www.googleapis.com/books/v1/volumes`, envía la
clave en la cabecera `X-Goog-Api-Key` y busca primero mediante `isbn:`. Si esa
consulta produce un falso vacío, prueba títulos obtenidos de Open Library o
ISBNdb y solo guarda un resultado cuyo ISBN coincida exactamente con el
solicitado o con su equivalente ISBN-10/ISBN-13.

Para comprobar la configuración desde la aplicación, ejecuta la fase de
metadatos sobre un libro con ISBN conocido y revisa el estado de Google Books
en la pantalla de revisión. Un `403` suele indicar una clave incorrecta, una
restricción incompatible, Books API deshabilitada o un proyecto distinto.

## Configurar Open Library

### Cuenta, registro y credenciales

Open Library no exige una cuenta ni entrega una clave de API para las
consultas que realiza esta aplicación. La cuenta gratuita de Open Library solo
es necesaria para funciones de usuario como editar registros o pedir libros
prestados; puede crearse, si se desea, en
[Crear una cuenta de Open Library](https://openlibrary.org/account/create).

Open Library sí solicita registrar las aplicaciones que usan sus API con
regularidad. Este registro describe el uso, pero no genera una clave, un token
ni una aprobación que haya que esperar:

- [Formulario oficial de registro de aplicaciones](https://docs.google.com/forms/d/1119CgdEWHtaAy2lOpUmtbe4S-w6xhJUin-nqP7Bl1XQ/viewform)
- [Normas, límites y listado oficial de API](https://openlibrary.org/developers/api)

### Rellenar el formulario

Para este repositorio se pueden usar las siguientes respuestas:

- **What is the url of your application**:
  `https://github.com/carlunio/media-catalog-books`.
- **A short description of your application**:
  `Open-source local application used to look up book metadata by ISBN for a
  personal catalog. Responses are cached in a local DuckDB database.`
- **What is the best contact email?**: el correo real de contacto que se
  guardará en `.env`.
- **What is the user-agent for your application?**:
  `media-catalog-books/1.0 (correo@ejemplo.com)`.
- **What APIs or services are used by your application?**:
  `ISBN/Edition JSON API, Search API (search.json), Covers API`.
- **Please describe the nature of your application**:
  **Free & Open Source**.
- En el compromiso de usar las API desde una sola IP, marca **I agree**.
- En la aceptación de que las API se ofrecen sin garantías ni soporte
  individual, marca **I agree**.
- En la solicitud opcional para mostrar la aplicación en la web de Open
  Library, puede marcarse **No thank you**.

Usa el mismo correo en el campo de contacto, en el `User-Agent` declarado y en
`OPENLIBRARY_CONTACT`. No hace falta escribir la contraseña de Open Library ni
ninguna otra credencial en el formulario.

### Configurar `.env`

Guarda el correo de contacto, sin comillas, en el `.env` local:

```dotenv
OPENLIBRARY_CONTACT=correo@ejemplo.com
OPENLIBRARY_MIN_INTERVAL_SECONDS=1
```

Reinicia después la aplicación:

```bash
make stop
make start
```

El código construye un `User-Agent` con el nombre y la versión de la
aplicación, añade ese correo y también envía el parámetro `email` a Search API.
Open Library publica un límite de una petición por segundo para peticiones sin
identificar y de tres por segundo para las identificadas. El proyecto mantiene
un segundo entre peticiones, por lo que queda dentro de ambos límites.

Para cada ISBN, la aplicación consulta `/isbn/{isbn}.json` para obtener la
edición concreta y `search.json` para recuperar un contexto reducido de la
obra. También puede usar `covers.openlibrary.org` para las imágenes. Las
respuestas se conservan en DuckDB, se reutilizan y no se hacen descargas
masivas, conforme a las recomendaciones de Open Library.

## Validación

`setup`, `launch`, `dev`, `update` y `smoke` rechazan antes de operar:

- líneas o comillas inválidas en `.env`;
- puertos fuera de rango, iguales o incoherentes con `API_URL`;
- números, booleanos o proveedores no válidos;
- un `PROJECT_ROOT` que no corresponda al repositorio.

Las claves desconocidas, los ejemplos de identidad sin personalizar y un
proveedor OpenAI sin clave se muestran como avisos. Nunca se imprimen valores de
secretos.

## Diagnóstico

```bash
python3 scripts/appctl.py doctor
```

El diagnóstico no modifica la base ni aplica migraciones. Comprueba el remoto y
la rama Git, sincronización del entorno con `pyproject.toml`, integridad de
los recursos,
permisos de las rutas, esquema de DuckDB, disponibilidad de puertos, procesos y
conectividad con Ollama. Los errores incluyen la acción de recuperación.

Para verificar un arranque completo sin usar la base real:

```bash
python3 scripts/appctl.py smoke
```

La prueba de humo crea una base y directorios temporales, arranca FastAPI y
Streamlit,
consulta sus rutas de salud y elimina después todos los datos temporales.
