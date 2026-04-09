# README.md - Proyecto 1: DataWarehouse Compras y Ventas

## Descripción del Proyecto

Construcción de un **DataWarehouse empresarial** que consolida datos de compras y ventas utilizando **SQL Server**, **SSIS** y **SSAS**. El proyecto implementa un modelo dimensional con Star Schema separado para cada área de negocio (compras y ventas), procesando archivos CSV con transformaciones en SSIS.

---

## Requisitos Técnicos Cumplidos

| Requisito | Implementación |
|-----------|---|
| **Herramientas** | Microsoft Visual Studio 2019+ con SSIS, SSAS |
| **Plataforma de Datos** | SQL Server 2019 (Docker) + Analysis Services |
| **Fuentes Heterogéneas** | Dos archivos CSV: compras.csv, ventas.csv |
| **Procesos** | ETL: Extracción → Transformación → Carga → Procesamiento |
| **Modelo de Datos** | Star Schema dimensional con SurrogateKeys en ambas BDs |
| **SSAS** | Cubos multidimensionales con dimensiones, jerarquías, medidas, perspectivas |
| **Conocimientos** | ETL, Data Warehouse, Modelado dimensional, SSAS, Control de Calidad |

---

## Arquitectura del Sistema

```
┌─────────────────────────────────────────────────────────┐
│                  FUENTES (CSV - SIN TRANSFORMACIÓN)     │
├─────────────────────────────────────────────────────────┤
│ compras.csv (Z→2, sin negativos)  │  ventas.csv (limpio)│
└──────────────┬─────────────────────────────────┬────────┘
               │                                  │
               └──────────────┬───────────────────┘
                              ▼
        ┌──────────────────────────────────────┐
        │  Proyecto1_SSIS.slnx                 │
        │  ├─ ETL Compras (transformaciones)  │
        │  └─ ETL Ventas (carga directa)      │
        └──────────────┬──────────────────────┘
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
1. `SQL/BDCompras.ddl` → Crea BD Compras_DB con Star Schema
2. `SQL/BDVentas.ddl` → Crea BD Ventas_DB con Star Schema

**Resultado esperado:** Dos bases de datos con tablas staging + dimensiones + hechos

**Nota:** Los scripts DDL no incluyen columnas de fecha de creación. Las dimensiones e hechos se crean limpios y listos para carga SSIS.

---

## Fase 2: Datos Origen (CSV)

### 2.1 Archivos CSV - Ingesta Directa

Los archivos CSV se cargan **directamente en SSIS sin conversión previa**:

**Compras:** `compras.csv`
- Transformaciones SSIS aplicadas: Reemplazar Z→2 en Fecha, ABS() en Unidades/PrecioUnitario, eliminar registros con valores nulos

**Ventas:** `ventas.csv`
- Datos limpios, carga directa sin transformaciones valor

**Ubicación:** Raíz del proyecto (Proyecto_1/)

---

## Fase 3: ETL - SSIS Proyecto1_SSIS.slnx

### 3.1 Estructura del Proyecto SSIS

Un único proyecto SSIS contiene dos flujos ETL:

#### **Flujo 1: ETL Compras**

```
compras.csv
    ↓
Flat File Source (delimitador: coma)
    ↓
Transformaciones:
  ├─ Data Conversion: Fechas (DT_STR → DT_DATE)
  │   └─ Z→2 en campo Fecha (ej: "Z3/08/2018" → "23/08/2018")
  ├─ Replace Z con 2 (Expression: REPLACE([Fecha],"Z","2"))
  ├─ Abs() en Unidades (eliminar negativos)
  ├─ Abs() en PrecioUnitario (eliminar negativos)
  └─ Lookups a 4 dimensiones (Producto, Proveedor, Sucursal, Fecha)
    ↓
Destino: Compras_DB.Fact_Compras
```

#### **Flujo 2: ETL Ventas**

```
ventas.csv
    ↓
Flat File Source (delimitador: coma)
    ↓
Transformaciones:
  └─ Lookups a 5 dimensiones (Producto, Cliente, Vendedor, Sucursal, Fecha)
    ↓
Destino: Ventas_DB.Fact_Ventas
```

### 3.2 Paquetes DTSX

Dentro del único proyecto `Proyecto1_SSIS.slnx`, hay dos flujos de datos:

1. **LimpiarDimensiones.dtsx** - ETL para poblar todas las dimensiones (Compras_DB + Ventas_DB)
2. **CargarHechos.dtsx** - ETL para cargar Fact_Compras (con transformaciones Z→2, ABS) + Fact_Ventas (carga directa)

---

## Fase 4: Modelo Analítico - SSAS Proyecto1_SSAS.slnx

### 4.1 Estructura de Cubos

### 4.1 Estructura de Cubos

Dos cubos multidimensionales (uno por BD):

| Elemento | Compras_Cube | Ventas_Cube |
|----------|---|---|
| **Dimensiones** | Fecha, Producto, Proveedor, Sucursal | Fecha, Producto, Cliente, Vendedor, Sucursal |
| **Medidas** | Unidades, Costo Total, Costo Promedio | Unidades, Ingresos, Precio Promedio |
| **Jerarquías** | Año→Trimestre→Mes, Categoría→Marca→Producto, Región→Depto→Sucursal | Idénticas a Compras |
| **Perspectivas** | Perspectiva_Compras | Perspectiva_Ventas |

### 4.2 Despliegue

```
Visual Studio → Build → Deploy → Process Full
```

Cubos lista para consultas MDX y reportes desde Power BI/Excel.

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
├─ SQLBDCompras.ddl                       (DDL: Compras_DB - sin FechaCreacion)
│  ├─ BDVentas.ddl                        (DDL: Ventas_DB - sin FechaCreacion)
│  └─ QueriesValidacion.sql               (QA: Queries de validación)
│
├─ Proyecto1_SSIS/
│  └─ Proyecto1_SSIS.slnx                 (Proyecto SSI
│  └─ Proyecto1_SSAS.slnx                 (Proyecto SSAS único)
│
├─ Datos_Origen/
│  ├─ compras.csv
│  └─ ventas.csv
│
├─ Plan.md                                 (Plan de implementación)
└─ README.md                               (Este archivo)
```

---

## Checklist de Implementación

### Infraestructura
- [ ] Docker container corriendo
- [ ] Conexión SSMS exitosa
- [ ] Compras_DB creada (BDCompras.ddl ejecutado)
- [ ] Ventas_DB creada (BDVentas.ddl ejecutado)

### Datos Origen
- [ ] compras.csv disponible en Proyecto_1/
- [ ] ventas.csv disponible en Proyecto_1/
- [ ] Archivos validados (encoding UTF-8, delimitador coma)

### SSIS ETL
- [ ] Proyecto1_SSIS.slnx creado en Visual Studio
- [ ] Paquete LimpiarDimensiones.dtsx ejecutado exitosamente
- [ ] Paquete CargarHechos.dtsx ejecutado (con trasformaciones Compras: Z→2, ABS)
- [ ] Sin errores de validación en SSIS

### Bases de Datos
- [ ] Dim_Fecha poblada (2557 registros: 2018-2024)
- [ ] Dim_Producto poblada
- [ ] Dim_Proveedor poblada (Compras_DB)
- [ ] Dim_Cliente poblada (Ventas_DB)
- [ ] Dim_Vendedor poblada (Ventas_DB)
- [ ] Dim_Sucursal poblada
- [ ] Fact_Compras poblada con transformaciones aplicadas
- [ ] Fact_Ventas poblada con carga directa

### SSAS
- [ ] Proyecto1_SSAS.slnx creado en Visual Studio
- [ ] Compras_Cube deployado y procesado
- [ ] Ventas_Cube deployado y procesado
- [ ] Perspectivas configuradas

### Validación QA
- [ ] QueriesValidacion.sql ejecutadas contra ambas BDs
- [ ] Cubos SSAS conectables desde SSMS
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
- Verificar conexiones a BD (checkbox OK en Connection Managers)
- Validar rutas archivos CSV (compras.csv, ventas.csv)
- Revisar columnas esperadas: Fecha, CodProducto, CodProveedor, Unidades, etc.
- Validar que transformaciones Z→2 y ABS() se aplican correctamente
- Revisar tabla log_Errores en BDs para detalles de rechazo

### SSAS - Cubo no procesa
```sql
-- En SSMS → Connect to Analysis Services (localhost:2383)
Process Database 'Proyecto_1_SSAS'
-- Seleccionar "Process Full"
```

### Integridad Referencial
```sql
-- En Compras_DB - Validar referencias a Dim_Producto
SELECT COUNT(*) FROM Fact_Compras
WHERE NOT EXISTS (SELECT 1 FROM Dim_Producto WHERE SK_Producto = Fact_Compras.SK_Producto)

-- En Ventas_DB - Validar referencias a Dim_Cliente  
SELECT COUNT(*) FROM Fact_Ventas
WHERE NOT EXISTS (SELECT 1 FROM Dim_Cliente WHERE SK_Cliente = Fact_Ventas.SK_Cliente)
```

### CSV no carga en SSIS
- Verificar delimitador es coma (,)
- Validar nombres columnas: Fecha, CodProveedor, NombreProveedor, CodProducto, etc.
- Asegurar archivo está en ruta correcta
- Probar con preview en Flat File Source

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
