# Recursos locales

Los ficheros de esta carpeta no se versionan. Se preparan en cada instalación
local mediante `make prepare-assets`, que también forma parte de `make setup`.

- `iso-639-3.tab` es obligatorio. Procede de la tabla oficial ISO 639-3
  publicada por SIL International:
  <https://iso639-3.sil.org/sites/iso639-3/files/downloads/iso-639-3.tab>.
- `dani.png` es un icono personalizado y opcional. Puede copiarse manualmente a
  esta carpeta o descargarse durante la preparación definiendo `APP_ICON_URL`
  en `.env`. Si no existe, Streamlit usa su icono por defecto.

Las capturas, notas y demás materiales locales también permanecen fuera de Git.
