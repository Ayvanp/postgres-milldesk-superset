# PROYECTO
Este proyecto implementa un proceso ETL (Extracción, Transformación y Carga) para gestionar y procesar datos de tickets desde una API externa hacia una base de datos PostgreSQL. El script extrae datos relevantes, los transforma según las necesidades específicas y los carga en las tablas correspondientes de la base de datos. Además, incluye funcionalidades para actualizar tablas relacionadas con SLA (Acuerdos de Nivel de Servicio).

## Instalación

### Requisitos Previos

- **Python 3.8** o superior: Asegúrate de tener Python instalado en tu sistema. Puedes verificar la versión instalada ejecutando `python --version` o `python3 --version`.

- **PostgreSQL**: Debes tener PostgreSQL instalado y en funcionamiento. Puedes descargarlo desde [aquí](https://www.postgresql.org/download/).
- **pip**: El gestor de paquetes de Python. Viene instalado por defecto con Python 3.4 y superiores.
- **Acceso a la API externa** con una **API_KEY** válida: Necesitas una clave de API para autenticarte con el servicio externo del cual se extraerán los datos.

### Pasos de Instalación

1. **Clonar el Repositorio**

   Clona este repositorio en tu máquina local utilizando Git:

    ```bash 
   git clone https://github.com/tu_usuario/tu_repositorio.git 
   ```

2. **Navegar al Directorio del Proyecto**

   Cambia al directorio del proyecto clonado:

    ```bash
    cd tu_repositorio 
    ```

3. **Crear y Activar un Entorno Virtual**

   Es recomendable usar un entorno virtual para gestionar las dependencias y evitar conflictos con otras instalaciones de Python:

   ```bash 
   python3 -m venv venv source venv/bin/activate
   ```

    - **Crear el entorno virtual**: `python3 -m venv venv` crea un entorno virtual llamado `venv`.
    - **Activar el entorno virtual**: `source venv/bin/activate` activa el entorno virtual en sistemas Unix/Linux. En Windows, usa `venv\Scripts\activate`.

4. **Instalar Dependencias**

    Instala las dependencias necesarias usando `pip`:

    ```bash 
    pip install -r requirements.txt 
    ```

    Este comando instalará todas las librerías listadas en el archivo `requirements.txt`, necesarias para ejecutar el script ETL.

5. **Configurar Variables de Entorno**

    Crea un archivo `.env` en el directorio `etl_script` con las siguientes variables:

    ```bash
    API_KEY=tu_api_key 
    BASE_URL=https://api.tu_servicio.com/ 
    DB_USER=tu_usuario_db DB_PASSWORD=tu_contraseña_db 
    DB_HOST=localhost D
    B_PORT=5432 
    DB_NAME=nombre_de_tu_base_de_datos
    ```

    - **API_KEY**: Tu clave de API para acceder al servicio externo.
    - **BASE_URL**: La URL base de la API externa.
    - **DB_USER**: Nombre de usuario para la base de datos PostgreSQL.
    - **DB_PASSWORD**: Contraseña para la base de datos PostgreSQL.
    - **DB_HOST**: Host donde está alojada la base de datos (por defecto, `localhost`).
    - **DB_PORT**: Puerto de conexión a la base de datos (por defecto, `5432`).
    - **DB_NAME**: Nombre de la base de datos donde se cargarán los datos.

    Asegúrate de reemplazar los valores con tus configuraciones reales.

## Uso

### Ejecutar el Script Manualmente

Para ejecutar el proceso ETL manualmente, sigue estos pasos:

1. **Activar el Entorno Virtual** (si no está ya activado): 
    ```bash
    source venv/bin/activate
    ```

2. **Navegar al Directorio del Proyecto**: 
    ```bash
    cd tu_repositorio
    ```

3. **Ejecutar el Script ETL**: 
    ```bash
    python etl_script/main.py
    ```

Esto iniciará el proceso ETL, que extraerá datos de la API, los transformará y los cargará en la base de datos configurada. Durante la ejecución, se generarán logs detallados que te permitirán monitorear el progreso y detectar posibles errores.

## Funcionalidad

El script principal (```main.py```) realiza las siguientes tareas:

1. **Conexión a la Base de Datos**: Establece una conexión segura con la base de datos PostgreSQL utilizando las credenciales proporcionadas en el archivo `.env`.

2. **Extracción de Datos desde la API**:
- **Estados de tickets**: Obtiene los diferentes estados que pueden tener los tickets.
- **Tickets por estado**: Recupera los tickets categorizados por su estado actual.
- **Tickets por período**: Extrae los tickets generados dentro de un rango de fechas específico.
- **Datos abiertos vs cerrados mensuales**: Compara la cantidad de tickets abiertos y cerrados cada mes.
- **Tickets por hora**: Analiza la distribución de tickets por hora del día.
- **Actividades por departamento**: Detalla las horas trabajadas y cargadas por cada departamento.
- **Satisfacción mensual promedio**: Calcula el promedio de satisfacción de los usuarios mensualmente.

3. **Transformación de Datos**:
- **Validación y limpieza de datos**: Asegura que los datos extraídos cumplen con los formatos y requisitos necesarios.
- **Conversión de formatos de fecha y números**: Convierte cadenas de texto a formatos de fecha y números adecuados para la base de datos.

4. **Carga de Datos en la Base de Datos**:
- **Inserción o actualización de datos**: Carga los datos transformados en las tablas correspondientes de la base de datos. Si las tablas ya contienen datos, los nuevos registros se añadirán o actualizarán según la configuración.

5. **Actualización de la Tabla SLA Próximos**: Actualiza la tabla `sla_proximos` con los tickets que tienen SLAs próximos a expirar, calculando el tiempo activo y restante en días.

Configuración de Tarea Cron

Para automatizar la ejecución del proceso ETL, se puede configurar una tarea cron que ejecute un script `.sh` a intervalos regulares.

Crear el Script Shell

1. **Crear el Archivo `run_etl.sh`**:

En el directorio raíz del proyecto, crea un archivo llamado `run_etl.sh` con el siguiente contenido:

```
#!/bin/bash
Activar el entorno virtual

source /ruta/a/tu_repositorio/venv/bin/activate
Navegar al directorio del proyecto

cd /ruta/a/tu_repositorio
Ejecutar el script ETL

python etl_script/main.py
Desactivar el entorno virtual

deactivate
```
   
- **source /ruta/a/tu_repositorio/venv/bin/activate**: Activa el entorno virtual. Reemplaza `/ruta/a/tu_repositorio/` con la ruta real donde clonaste el repositorio.
- **cd /ruta/a/tu_repositorio**: Navega al directorio del proyecto.
- **python etl_script/main.py**: Ejecuta el script ETL.
- **deactivate**: Desactiva el entorno virtual después de la ejecución.

2. **Hacer el Script Ejecutable**:

Otorga permisos de ejecución al script:

``` `chmod +x run_etl.sh`, ```

Configurar la Tarea Cron

1. **Editar el Archivo Crontab**:

Abre el editor de crontab ejecutando:

``` `crontab -e` ```
2. **Añadir la Línea de Tarea Cron**:

Para ejecutar el script cada 5 mins entre las 10:00 y 21:00 HRS, añade la siguiente línea:

```
*/5 X * * * /ruta/a/tu_repositorio/run_etl.sh >> /ruta/a/tu_repositorio/etl_cron.log 2>&1
```

- **0 2 * * ***: Configura la tarea para que se ejecute a las 2:00 AM todos los días.
- **/ruta/a/tu_repositorio/run_etl.sh**: Ruta completa al script `run_etl.sh`.
- **>> /ruta/a/tu_repositorio/etl_cron.log 2>&1**: Redirige la salida estándar y de error al archivo `etl_cron.log` para registro de logs.

3. **Guardar y Salir**:

Después de añadir la línea, guarda los cambios y cierra el editor. La tarea cron estará activa y se ejecutará según lo programado.

Verificar la Configuración

Después de configurar la tarea cron, puedes verificar que esté correctamente instalada ejecutando:

``` `crontab -l` ```
Este comando listará todas las tareas cron configuradas para el usuario actual. Asegúrate de que la línea que añadiste aparece en la lista.

Logs

El proceso ETL genera logs detallados en el archivo `etl_process.log` ubicado en el directorio raíz del proyecto. Además, los logs de la tarea cron se almacenan en `etl_cron.log` si se configuró la redirección en el script `.sh`.

- **etl_process.log**: Contiene información sobre el inicio y fin del proceso ETL, así como mensajes de éxito o error durante la ejecución.
- **etl_cron.log**: Registra la salida y posibles errores de la ejecución automática del script a través de cron.

Soporte

Para cualquier problema o consulta, por favor abre un **issue** en el repositorio o contacta al mantenedor del proyecto. Asegúrate de proporcionar detalles claros y concisos sobre el problema para facilitar su resolución.

Licencia

Este proyecto está licenciado bajo la **Licencia MIT**. Consulta el archivo `LICENSE` para más detalles.