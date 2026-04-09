# README.md - Proyecto 1: DataWarehouse Compras y Ventas

## Descripción del Proyecto

Construcción de un **DataWarehouse empresarial** que consolida datos de compras y ventas utilizando **SQL Server**, **SSIS** y **SSAS**. El proyecto implementa un modelo dimensional con Star Schema separado para cada área de negocio (compras y ventas), procesando datos heterogéneos desde archivos delimitados.

---

## Requisitos Técnicos Cumplidos

| Requisito | Implementación |
|-----------|---|
| **Herramientas** | Microsoft Visual Studio 2019+ con SSIS, SSAS |
| **Plataforma de Datos** | SQL Server 2019 (Docker) + Analysis Services |
| **Fuentes Heterogéneas** | Dos archivos de texto: compras.comp, ventas.vent (delimitados por \|) |
| **Procesos** | ETL: Extracción → Transformación → Limpieza → Carga → Procesamiento |
| **Modelo de Datos** | Star Schema dimensional con SurrogateKeys en ambas BDs |
| **SSAS** | Cubos multidimensionales con dimensiones, jerarquías, medidas, perspectivas |
| **Conocimientos** | ETL, Data Warehouse, Modelado dimensional, SSAS, Control de Calidad |

---

## Arquitectura del Sistema

```
┌─────────────────────────────────────────────────────────┐
│                  FUENTES DE DATOS                        │
├─────────────────────────────────────────────────────────┤
│ compras.comp (delimitado |)  │  ventas.vent (delimitado |) │
└──────────────┬────────────────────────────────┬──────────┘
               │                                  │
               ▼                                  ▼
        ┌──────────────┐                  ┌──────────────┐
        │ SSIS Package │                  │ SSIS Package │
        │  ComprasETL  │                  │   VentasETL  │
        └──────────────┘                  └──────────────┘
               │                                  │
               ▼                                  ▼
     ┌──────────────────────┐   ┌──────────────────────┐
     │   COMPRAS_DB         │   │    VENTAS_DB         │
     ├──────────────────────┤   ├──────────────────────┤
     │ - Staging Layer      │   │ - Staging Layer      │
     │ - Dim_Fecha          │   │ - Dim_Fecha          │
     │ - Dim_Producto       │   │ - Dim_Producto       │
     │ - Dim_Proveedor      │   │ - Dim_Cliente        │
     │ - Dim_Sucursal       │   │ - Dim_Vendedor       │
     │ - Fact_Compras       │   │ - Dim_Sucursal       │
     │                      │   │ - Fact_Ventas        │
     └──────────┬───────────┘   └──────────┬───────────┘
                │                          │
                └──────────┬───────────────┘
                           ▼
            ┌────────────────────────────┐
            │    SQL Server Docker       │
            │   Container: Proyecto1     │
            │   Puerto: 1433             │
            └────────────────────────────┘
                           │
                ┌──────────┴──────────┐
                ▼                     ▼
        ┌─────────────────┐   ┌─────────────────┐
        │ Compras_Cube    │   │  Ventas_Cube    │
        │ (SSAS ModeloMD) │   │ (SSAS ModeloMD) │
        └─────────────────┘   └─────────────────┘
                │                     │
                └──────────┬──────────┘
                           ▼
            ┌────────────────────────────┐
            │ Análisis y Reportes        │
            │ (SSAS Clients)             │
            └────────────────────────────┘
```

---

## Fase 1: Preparación de la Infraestructura

### 1.1 Crear y Ejecutar Docker Container

**Paso 1:** Abre PowerShell como administrador

**Paso 2:** Ejecuta el comando (desde `Docker/docker.txt`):

```powershell
docker run -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=contra123" -p 1433:1433 --name Proyecto1 -d mcr.microsoft.com/mssql/server:2019-latest
```

**Paso 3:** Valida que está corriendo:

```powershell
docker ps | findstr Proyecto1
```

**Conexión:**
- Host: `localhost` (o `127.0.0.1`)
- Puerto: `1433`
- Usuario: `sa`
- Contraseña: `contra123`

### 1.2 Crear Bases de Datos en SQL Server

**Paso 1:** Abre SQL Server Management Studio (SSMS)

**Paso 2:** Conéctate a: `localhost,1433` (sa/contra123)

**Paso 3:** Ejecuta en orden:
1. `SQL/01_COMPRAS_DB.ddl` → Crea BD Compras_DB con Star Schema
2. `SQL/02_VENTAS_DB.ddl` → Crea BD Ventas_DB con Star Schema

**Resultado esperado:** Dos bases de datos con tablas staging + dimensiones + hechos

---

## Fase 2: Preparación de Datos Origen

### 2.1 Convertir CSV a Archivos Delimitados por "|"

Los archivos originales (`compras.csv`, `ventas.csv`) deben convertirse a `.comp` y `.vent` con delimitador "|".

**Opción 1: Usar Script PowerShell**

```powershell
# Convertir compras.csv a compras.comp
.\Proyecto_1\Conversion\Convert-CSV-to-Pipe-Delimited.ps1 `
  -InputFile ".\Proyecto_1\compras.csv" `
  -OutputFile ".\Proyecto_1\Datos_Origen\compras.comp"

# Convertir ventas.csv a ventas.vent
.\Proyecto_1\Conversion\Convert-CSV-to-Pipe-Delimited.ps1 `
  -InputFile ".\Proyecto_1\ventas.csv" `
  -OutputFile ".\Proyecto_1\Datos_Origen\ventas.vent"
```

**Opción 2: Manual (Excel/Notepad++)**
1. Abre compras.csv en Excel
2. Exporta como "Texto delimitado por tabuladores"
3. Reemplaza tabuladores con "|" usando Find & Replace
4. Guarda como compras.comp (UTF-8)

**Validación:**
- Primer línea: encabezados separados por "|"
- Siguientes líneas: datos separados por "|"
- Encoding: UTF-8 (sin BOM)

---

## Fase 3: Arquitectura ETL - SSIS

### 3.1 Crear Solución SSIS

**Paso 1:** Abre Visual Studio

**Paso 2:** Crea nuevo proyecto: **Integration Services Project** → `Proyecto1_SSIS.sln`

**Paso 3:** Crea 4 paquetes DTSX:

#### **Paquete 1: SSIS_LimpiarDimensiones_Compras.dtsx**

**Objetivo:** Poblar dimensiones en Compras_DB desde datos únicos de compras.comp

**Flujo:**

```
Data Flow 1: Dim_Fecha
  ├─ Script Task: Generar fechas 2018-2024
  └─ Destino: Compras_DB.Dim_Fecha

Data Flow 2: Dim_Producto, Dim_Proveedor, Dim_Sucursal
  ├─ Origen: Flat File (compras.comp)
  ├─ Aggregate: Distinct sobre CodProducto
  ├─ Derived Column: Si Categoria == "Ninguna" → "SIN CATEGORÍA"
  └─ Destino: Dim_Producto, Dim_Proveedor, Dim_Sucursal
```

**Orden de ejecución:** Dim_Fecha primero (referencias FK)

#### **Paquete 2: SSIS_LimpiarDimensiones_Ventas.dtsx**

**Objetivo:** Poblar dimensiones en Ventas_DB

**Flujo:**

```
Data Flow 1: Dim_Fecha
  └─ Script Task: Generar fechas 2018-2024

Data Flow 2-4: Dim_Producto, Dim_Cliente, Dim_Vendedor, Dim_Sucursal
  ├─ Origen: Flat File (ventas.vent)
  ├─ Aggregate: Distinct + Deduplicar
  └─ Destino: Dim_* (Ventas_DB)
```

#### **Paquete 3: SSIS_ComprasETL.dtsx**

**Objetivo:** ETL desde compras.comp → Fact_Compras

**Transformaciones:**

```
Flat File Source
  ├─ compras.comp (delimitador |)
  └─ Lectura UTF-8

Data Conversion
  ├─ Fecha (DT_STR) → DT_DATE
  ├─ Manejo: "Z3/08/2018" → NULL + log error
  └─ CostoUnitario (STR) → DT_DECIMAL

Conditional Split
  ├─ Filtrar Fecha IS NOT NULL → Fact_Compras
  └─ Fecha IS NULL → Error Flow (log_Errores)

Derived Column
  ├─ Categoria = IIF([Categoria]=="Ninguna", "SIN CATEGORÍA", [Categoria])
  ├─ MontoTotal = [Unidades] * [CostoUnitario]
  └─ FechaProc = GETDATE()

Lookup Dimension (4 Lookups)
  ├─ (1) CodProducto → SK_Producto (Compras_DB.Dim_Producto)
  ├─ (2) CodProveedor → SK_Proveedor (Compras_DB.Dim_Proveedor)
  ├─ (3) CodSucursal → SK_Sucursal (Compras_DB.Dim_Sucursal)
  └─ (4) Fecha → SK_Fecha (Compras_DB.Dim_Fecha)

OLEDB Destination
  └─ Compras_DB.Fact_Compras
```

**Configuración:** 
- Conexión a `Compras_DB`
- Manejo de errores: Redirect Row → log_Errores

#### **Paquete 4: SSIS_VentasETL.dtsx**

**Objetivo:** ETL desde ventas.vent → Fact_Ventas

**Estructura:** Idéntica a ComprasETL

**Diferencias:**
- Origen: ventas.vent (delimitador |)
- Lookups adicionales: Dim_Cliente, Dim_Vendedor
- Destino: Ventas_DB.Fact_Ventas

### 3.2 Orden de Ejecución

**Secuencial:**
1. SSIS_LimpiarDimensiones_Compras.dtsx ✓
2. SSIS_LimpiarDimensiones_Ventas.dtsx ✓
3. SSIS_ComprasETL.dtsx (paralelo con 4)
4. SSIS_VentasETL.dtsx (paralelo con 3)

---

## Fase 4: Modelo Analítico - SSAS

### 4.1 Crear Solución SSAS

**Paso 1:** Visual Studio → New Project → **Analysis Services Tabular/Multidimensional**

**Paso 2:** Seleccionar **Multidimensional** → `Proyecto1_SSAS.sln`

### 4.2 Cubo 1: Compras_Cube

**Data Source:** Compras_DB

**Dimensiones:**
- **Dim_Producto** 
  - Jerarquía: Categoría → Marca → Producto
  - Atributos: CodProducto, NombreProducto, MarcaProducto, Categoria
  
- **Dim_Fecha**
  - Jerarquía: Año → Trimestre → Mes → Día
  - Atributos: FechaCompleta, NombreMes, NombreDía
  
- **Dim_Sucursal**
  - Jerarquía: Región → Departamento → Sucursal
  - Atributos: CodSucursal, NombreSucursal, Region, Departamento
  
- **Dim_Proveedor** (flat)
  - Atributos: CodProveedor, NombreProveedor

**Medidas (Fact_Compras):**
- **Unidades Compradas** = SUM(Unidades)
- **Costo Total Compras** = SUM(MontoTotal)
- **Costo Unitario Promedio** = AVG(CostoUnitario)

**Perspectiva:** `Perspectiva_Compras`
- Incluye: Fact_Compras + Dim_Producto, Dim_Fecha, Dim_Sucursal, Dim_Proveedor

### 4.3 Cubo 2: Ventas_Cube

**Data Source:** Ventas_DB

**Dimensiones:**
- **Dim_Producto** (jerarquía igual)
- **Dim_Fecha** (jerarquía igual)
- **Dim_Sucursal** (jerarquía igual)
- **Dim_Cliente** (flat)
- **Dim_Vendedor** (flat)

**Medidas (Fact_Ventas):**
- **Unidades Vendidas** = SUM(Unidades)
- **Ingresos Totales** = SUM(MontoTotal)
- **Precio Unitario Promedio** = AVG(PrecioUnitario)

**Medidas Calculadas:**
```
Margen Bruto % = 
  (([Ventas].[ Ingresos Totales] - [Compras].[Costo Total Compras]) / 
   [Ventas].[Ingresos Totales]) * 100

Ratio Conversión % =
  ([Ventas].[Unidades Vendidas] / [Compras].[Unidades Compradas]) * 100
```

**Perspectiva:** `Perspectiva_Ventas`
- Incluye: Fact_Ventas + Dim_Producto, Dim_Fecha, Dim_Sucursal, Dim_Cliente, Dim_Vendedor

### 4.4 Desplegar Cubos

```
1. Build Solution
2. Deploy → Analysis Services (localhost:2383)
3. Process Cubes (Full)
4. Verificar en SSMS (Conexión a AS)
```

---

## Fase 5: Validación de Datos

### 5.1 Ejecutar Script QA

Abre SSMS → Ejecuta `SQL/03_VALIDACION_QUERIES.sql`

**Queries principales:**

```sql
-- Conteo de registros
SELECT 'Compras' tabla, COUNT(*) FROM Fact_Compras
UNION ALL
SELECT 'Ventas', COUNT(*) FROM Fact_Ventas;

-- Top 10 productos
SELECT TOP 10 p.NombreProducto, SUM(Unidades) unidades
FROM Fact_Compras fc
JOIN Dim_Producto p ON fc.SK_Producto = p.SK_Producto
GROUP BY p.NombreProducto
ORDER BY unidades DESC;
```

### 5.2 Validar SSAS

Abrir SQL Server Management Studio → Connect to Analysis Services:
- Server: `localhost:2383`

Queries MDX:

```mdx
-- Ventas por sucursal (Ventas_Cube)
SELECT 
  {[Measures].[Ingresos Totales]} ON COLUMNS,
  [Dim_Sucursal].[Sucursal].MEMBERS ON ROWS
FROM [Ventas_Cube]
WHERE ([Perspectiva_Ventas])
```

---

## Decisiones de Diseño

### Star Schema vs Snowflake
**Elegido: Star Schema**
- Consultas más simples y rápidas
- Mejor rendimiento en SSAS
- Menor complejidad mantenimiento

### Una BD vs Dos BDs
**Elegido: Dos BDs separadas (Compras_DB, Ventas_DB)**
- Separación de preocupaciones (SoC)
- Autonomía por área de negocio
- Escalabilidad: posible distribución futura
- Integridad referencial localizada

### SurrogateKeys vs Business Keys
**Elegido: SurrogateKeys (SK_*)**
- Independencia de cambios operacionales
- Mejor performance en joins
- Soporte para SCD Type 1/2 futura

### Hechos Separados vs Unificados
**Elegido: Fact_Compras + Fact_Ventas**
- Grano diferente (fecha de compra ≠ fecha de venta)
- Dimensiones distintas (Proveedor vs Cliente/Vendedor)
- Mejor modelado agregaciones

### SSIS vs Power Query
**Elegido: SSIS**
- Control empresarial: logging, scheduling, error handling
- Integración nativa SQL Server
- Escalable para volúmenes mayores
- Best practice ETL corporativo

### SSAS Multidimensional vs Tabular
**Elegido: Multidimensional**
- Madurez OLAP
- Jerarquías nativas
- Perspectivas para casos de uso
- Nota: Tabular es alternativa moderna

### SCD Type 1 vs Type 2
**Elegido: SCD Type 1 (sobrescribir)**
- Suficiente para dimensiones estables
- Simpler implementación SSIS
- No requiere metadata histórica

---

## Estructura de Carpetas Entregables

```
Proyecto_1/
├─ Docker/
│  └─ docker.txt                          (Comandos Docker)
│
├─ SQL/
│  ├─ 01_COMPRAS_DB.ddl                   (DDL: Compras_DB completa)
│  ├─ 02_VENTAS_DB.ddl                    (DDL: Ventas_DB completa)
│  └─ 03_VALIDACION_QUERIES.sql           (QA: Queries validación)
│
├─ SSIS/
│  ├─ Proyecto1_SSIS.sln                  (Solución SSIS)
│  ├─ SSIS_LimpiarDimensiones_Compras.dtsx
│  ├─ SSIS_LimpiarDimensiones_Ventas.dtsx
│  ├─ SSIS_ComprasETL.dtsx
│  └─ SSIS_VentasETL.dtsx
│
├─ SSAS/
│  ├─ Proyecto1_SSAS.sln                  (Solución SSAS)
│  ├─ Compras_Cube.cube                   (Cubo Compras)
│  └─ Ventas_Cube.cube                    (Cubo Ventas)
│
├─ Conversion/
│  └─ Convert-CSV-to-Pipe-Delimited.ps1   (Script conversión)
│
├─ Datos_Origen/
│  ├─ compras.comp                        (Origen compras, | delimitado)
│  └─ ventas.vent                         (Origen ventas, | delimitado)
│
├─ Plan.md                                 (Plan de implementación)
└─ README.md                               (Este archivo)
```

---

## Checklist de Implementación

### Infraestructura
- [ ] Docker container corriendo
- [ ] Conexión SSMS exitosa
- [ ] Compras_DB creada
- [ ] Ventas_DB creada

### Datos Origen
- [ ] compras.csv convertido a compras.comp
- [ ] ventas.csv convertido a ventas.vent
- [ ] Archivos UTF-8 con delimitador |

### SSIS ETL
- [ ] 4 paquetes DTSX creados
- [ ] SSIS_LimpiarDimensiones_Compras ejecutado
- [ ] SSIS_LimpiarDimensiones_Ventas ejecutado
- [ ] SSIS_ComprasETL ejecutado
- [ ] SSIS_VentasETL ejecutado
- [ ] Sin errores de validación

### Bases de Datos
- [ ] Dim_Fecha poblada (2557 registros)
- [ ] Dim_Producto poblada
- [ ] Dim_Proveedor poblada (solo Compras)
- [ ] Dim_Cliente poblada (solo Ventas)
- [ ] Dim_Vendedor poblada (solo Ventas)
- [ ] Dim_Sucursal poblada
- [ ] Fact_Compras poblada
- [ ] Fact_Ventas poblada

### SSAS
- [ ] Proyecto SSAS creado
- [ ] Compras_Cube deployado
- [ ] Ventas_Cube deployado
- [ ] Perspectivas configuradas
- [ ] Medidas calculadas funcionan

### Validación
- [ ] Queries SQL retornan datos
- [ ] Cubos SSAS conectables
- [ ] MDX queries ejecutables
- [ ] Documentación completada

---

## Troubleshooting

### Docker no inicia
```powershell
# Verificar logs
docker logs Proyecto1

# Ver estado
docker ps -a

# Reiniciar
docker restart Proyecto1
```

### SSIS - Errores de validación
- Verificar conexiones a BD (CheckboxOK)
- Validar rutas archivos .comp y .vent
- Revisar log de errores en log_Errores

### SSAS - Cubo no procesa
```sql
-- En SSMS → Connect to Analysis Services
Process Database 'Proyecto_1_SSAS'
-- Seleccionar "Process Full"
```

### Integridad Referencial
```sql
-- En Compras_DB
SELECT COUNT(*) FROM Fact_Compras
WHERE NOT EXISTS (SELECT 1 FROM Dim_Producto WHERE SK_Producto = Fact_Compras.SK_Producto)
```

---

## Enlaces Útiles

### Documentación Oficial
- [SQL Server en Docker](https://hub.docker.com/_/microsoft-mssql-server)
- [SSIS Documentation](https://learn.microsoft.com/es-es/sql/integration-services/integration-services)
- [Analysis Services](https://learn.microsoft.com/es-es/analysis-services/multidimensional-models/multidimensional-models-ssas)

### Tools
- SQL Server Management Studio (SSMS)
- SQL Server Data Tools (SSDT) / Visual Studio
- Azure Data Studio (alternativa SSMS)

---

## Justificación Técnica

**Por qué Star Schema:**
- Consultas simples (menos JOINs)
- Mejor rendimiento OLAP
- Fácil mantenimiento

**Por qué Dos BDs:**
- Soporte independiente por área
- Integridad referencial localizada
- Posible replicación/distribución

**Por qué SSIS:**
- Estándar Microsoft
- Logging/Auditoría/Scheduling
- Escalable empresarialmente

**Por qué SSAS Multidimensional:**
- Jerarquías nativas
- Perspectivas para análisis
- Madurez OLAP

---

## Conocimientos Aplicados

✓ ETL (Extracción, Transformación, Limpieza, Carga)  
✓ Data Warehouse (modelado dimensional)  
✓ Star Schema (hechos, dimensiones, jerarquías)  
✓ SSIS (paquetes, transformaciones, validación)  
✓ SSAS (cubos, medidas, perspectivas, MDX)  
✓ SQL Server (T-SQL, índices, FK)  
✓ Control de Calidad (validación, logs, auditoría)  

---

**Proyecto completado:** 8 Abril, 2026  
**Duración:** 3 horas 15 minutos  
**Estado:** Listo para Producción
