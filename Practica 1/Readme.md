# Sistema ETL para Análisis de Registros de Vuelos

## Descripción General

Este proyecto implementa un proceso ETL completo (Extracción, Transformación y Carga) para integrar y analizar datos de registros de vuelos. La solución utiliza Python para el procesamiento de datos y SQL Server para almacenarlos en un modelo multidimensional diseñado para análisis de inteligencia de negocios.

## Objetivos

-  Extraer datos de múltiples fuentes (CSV)
-  Transformar y limpiar datos heterogéneos
-  Cargar datos en un modelo multidimensional de SQL Server
-  Generar indicadores analíticos
-  Visualizar resultados con Matplotlib

##  Estructura del Proyecto

```
Practica 1/
├── ELT.py                    # Aplicación principal del proceso ETL
├── visualizacion.py          # Script de visualización con Matplotlib
├── consultas_analisis.sql    # Consultas SQL para análisis
├── Dataset 1.csv             # Datos de fuente 1
├── Dataset 2.csv             # Datos de fuente 2
├── Script.sql                # Creación del modelo multidimensional
├── requirements.txt          # Dependencias del proyecto
└── README.md                 # Este archivo
```

## Arquitectura del Modelo Multidimensional

### Esquema Estrella (Star Schema)

```
                    Dim_Pasajero
                          |
            Dim_CanalVenta | Dim_Tiempo | Dim_MetodoPago
                      \    |    /    \    /
                        Hecho_Venta
                             |
                       Dim_Moneda
```

### Tablas de Dimensiones

| Tabla | Campos | Descripción |
|-------|--------|-------------|
| **Dim_Pasajero** | id_pasajero, passenger_id, passenger_gender, passenger_age, passenger_nationality | Información demográfica de pasajeros |
| **Dim_Tiempo** | id_tiempo, booking_datetime, anio, mes, dia, hora | Descomposición temporal de fechas |
| **Dim_CanalVenta** | id_canal, sales_channel | Canales de venta (APP, WEB, AEROPUERTO, etc.) |
| **Dim_MetodoPago** | id_metodo_pago, payment_method | Métodos de pago utilizados |
| **Dim_Moneda** | id_moneda, currency | Divisas de transacción |

### Tabla de Hechos

| Campo | Tipo | Descripción |
|-------|------|-------------|
| id_venta | PK | Identificador único de venta |
| id_pasajero | FK | Referencia a Dim_Pasajero |
| id_tiempo | FK | Referencia a Dim_Tiempo |
| id_canal | FK | Referencia a Dim_CanalVenta |
| id_metodo_pago | FK | Referencia a Dim_MetodoPago |
| id_moneda | FK | Referencia a Dim_Moneda |
| ticket_price | DECIMAL | Precio del boleto en moneda local |
| ticket_price_usd_est | DECIMAL | Precio estimado en USD |
| bags_total | INT | Total de maletas |
| bags_checked | INT | Maletas facturadas |

## Instalación y Configuración

### Requisitos Previos

1. **SQL Server 2022** ejecutándose en Docker (o instalado localmente)
2. **Python 3.8+**
3. **ODBC Driver 18 for SQL Server**

### Paso 1: Crear la Base de Datos en SQL Server

Ejecuta el contenedor Docker:

```bash
docker run -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=PasswordSegura123!" \
  -p 1433:1433 --name sqlserver-practica \
  -v sql_data:/var/opt/mssql \
  -d mcr.microsoft.com/mssql/server:2022-latest
```

Luego ejecuta el script de creación de tablas (`Script.sql`) en SQL Server Management Studio o Azure Data Studio.

### Paso 2: Instalar Dependencias de Python

```bash
pip install -r requirements.txt
```

**Nota:** En Windows, es posible que necesites instalar manualmente ODBC Driver 17:
```bash
# Descargar desde: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
```

### Paso 3: Configurar Credenciales

Edita el archivo `ELT.py` y verificar la sección `DATABASE_CONFIG`:

```python
DATABASE_CONFIG = {
    'Server': 'localhost,1433',
    'Database': 'master',
    'UID': 'sa',
    'PWD': 'PasswordSegura123!',
    'Driver': '{ODBC Driver 18 for SQL Server}'
}
```

##  Ejecución del Proceso ETL

### Ejecutar el Proceso Completo

```bash
python ELT.py
```

**Salida esperada:**
```
============================================================
INICIANDO PROCESO ETL
============================================================

====== INICIANDO FASE DE EXTRACCIÓN ======
Extrayendo datos de: Dataset 1.csv
 Registros extraídos de Dataset 1.csv: 500
...

====== INICIANDO FASE DE TRANSFORMACIÓN ======
 Registros después de transformación: 450
 Transformación completada exitosamente

====== INICIANDO FASE DE CARGA ======
 450 pasajeros insertados en Dim_Pasajero
...
 PROCESO ETL COMPLETADO EXITOSAMENTE
============================================================
```

### Generar Visualizaciones

```bash
python visualizacion.py
```

**Archivos generados:**
- `01_distribucion_genero.png` - Gráfico de barras por género
- `02_canales_venta.png` - Gráficos de cantidad e ingresos por canal
- `03_metodos_pago.png` - Gráfico de pastel de métodos
- `04_nacionalidades_top.png` - Top 10 nacionalidades
- `05_rango_edades.png` - Distribución por edad
- `06_ventas_por_mes.png` - Evolución temporal de ventas
- `08_resumen_ejecutivo.png` - KPIs principales

## Consultas Analíticas Disponibles

El archivo `consultas_analisis.sql` contiene 10 consultas:

1. **Validación de carga** - Registros por tabla (Pasajeros, Tiempos, Canales, Métodos, Monedas, Ventas)
2. **Total de vuelos** - Cantidad total de transacciones
3. **Distribución por género** - Análisis demográfico con porcentajes
4. **Nacionalidades más frecuentes** - Top 10 por volumen de ventas
5. **Canales de venta** - Total de ventas, ingresos e ingreso promedio por canal
6. **Métodos de pago** - Total de transacciones, ingresos e ingreso promedio por método
7. **Distribución de edad** - Segmentación etaria (0-18, 19-30, 31-45, 46-60, 60+) con precio promedio
8. **Análisis de maletas** - Total de maletas y maletas facturadas con promedios
9. **Top 10 nacionalidades por ingresos** - Países con mayores ingresos totales
10. **Estadísticas generales** - Resumen ejecutivo (pasajeros únicos, total ventas, ingresos, precio promedio/mín/máx, maletas)

### Ejemplo de Ejecución de Consulta

```sql
-- Ejecutar en SQL Server Management Studio
USE Semi2_P1;
GO

-- Obtener las nacionalidades más frecuentes
SELECT TOP 10
    dp.passenger_nationality AS Nacionalidad,
    COUNT(*) AS Total,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM Hecho_Venta), 2) AS Porcentaje
FROM Hecho_Venta hv
INNER JOIN Dim_Pasajero dp ON hv.id_pasajero = dp.id_pasajero
WHERE dp.passenger_nationality != 'UNKNOWN'
GROUP BY dp.passenger_nationality
ORDER BY Total DESC;
```

## Justificación del Diseño Multidimensional

### Propósito y Ventajas

El esquema estrella optimiza consultas OLAP para análisis empresarial:

- **Rendimiento:** JOINs simples con índices en claves foráneas
- **Escalabilidad:** Dimensiones independientes, carga incremental posible  
- **Usabilidad:** Semántica de negocio clara para usuarios finales
- **Calidad:** Normalización de datos heterogéneos (géneros, fechas, monedas con conversión a USD)

### ¿Por qué Modelo Estrella y no Galaxia?

**Modelo Estrella:** Una tabla de hechos central + dimensiones independientes

**Modelo Galaxia:** Múltiples tablas de hechos compartiendo dimensiones

**3 Razones clave para usar Estrella:**

1. **Un único proceso de negocio:** El proyecto solo maneja ventas de boletos. Un modelo galaxia es necesario cuando hay múltiples procesos independientes (ej: ventas, reembolsos, cambios de reserva) que comparten dimensiones. Aquí no aplica.

2. **Simplicidad y rendimiento:** Una tabla de hechos única evita JOINs complejos entre múltiples tablas de hechos y reduce la complejidad de mantenimiento. Las consultas son más rápidas y el modelo es más fácil de entender.

3. **Escalabilidad sin complejidad:** Si en el futuro se agregan otros procesos de negocio, el modelo puede extenderse sin romper lo existente. Se puede pasar a galaxia cuando realmente sea necesario, no antes.

### Características del Modelo

- **Datos Normalizados:** Género (M/F/X), Fechas (múltiples formatos parseados), Edades (validadas 0-120)
- **Tabla de Hechos Desnormalizada:** Incluye `ticket_price_usd_est` para comparabilidad sin cálculos repetitivos
- **Manejo de Calidad:** Valores por defecto (UNKNOWN, OTHER), eliminación de duplicados por composite key, validación de rangos

## Fases del Proceso ETL

### Fase 1: Extracción (Extract)

- Lee archivos CSV con separador `;`
- Soporta múltiples fuentes de datos
- Elimina duplicados basados en `passenger_id` y `booking_datetime`
- Manejo de excepciones por archivo

**Archivo:** `ExtractorCSV` en `ELT.py`

### Fase 2: Transformación (Transform)

**Limpieza de datos:**
- **Género:** Normaliza "M", "F", "Masculino", "Femenino", etc. → "M", "F", "X"
- **Fechas:** Parsea múltiples formatos (DD/MM/YYYY, MM-DD-YYYY, ISO 8601)
- **Precios:** Convierte comas por puntos
- **Edades:** Valida rango 0-120 años
- **Valores faltantes:** Rellena con valores por defecto

**Archivo:** `Transformer` en `ELT.py`

### Fase 3: Carga (Load)

- Conecta a SQL Server via PyODBC
- Inserta datos en dimensiones (con validación de claves primarias)
- Carga la tabla de hechos con referencias a dimensiones
- Transacciones ACID para integridad de datos
- Manejo de conflictos de integridad

**Archivo:** `Loader` en `ELT.py`

## Registro de Errores

El sistema genera un archivo `etl_process.log` con:
- Timestamp de cada operación
- Nivel de severidad (INFO, WARNING, ERROR)
- Detalles de errores y excepciones
- Cantidad de registros procesados

**Ejemplo de log:**
```
2024-01-15 10:30:45,123 - INFO - ====== INICIANDO FASE DE EXTRACCIÓN ======
2024-01-15 10:30:45,124 - INFO - Extrayendo datos de: Dataset 1.csv
2024-01-15 10:30:46,542 - INFO - ✓ Registros extraídos de Dataset 1.csv: 500
```

## Indicadores Principales

El sistema genera los siguientes KPIs:

| KPI | Fórmula | Utilidad |
|-----|---------|----------|
| **Total de Pasajeros** | COUNT(DISTINCT id_pasajero) | Alcance del negocio |
| **Total de Ventas** | COUNT(*) | Volumen de transacciones |
| **Ingresos Totales** | SUM(ticket_price_usd_est) | Resultado económico |
| **Ticket Promedio** | AVG(ticket_price_usd_est) | Valor medio por venta |
| **Distribución por Canal** | % por sales_channel | Efectividad de canales |
| **Concentración de PaíS** | % top nacionalidades | Mercados prioritarios |

## Solución de Problemas

### Error: "Cannot open database"
```
Solución: Asegurate que SQL Server está corriendo y el contenedor Docker está activo
docker ps  # Verificar
```

### Error: "ODBC Driver not found"
```
Solución: Instala ODBC Driver 17 desde:
https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
```

### Error: "Connection refused"
```
Solución: Verifica que el puerto 1433 está abierto y SQL Server está escuchando
sqlcmd -S localhost,1433 -U sa -P PasswordSegura123!
```

