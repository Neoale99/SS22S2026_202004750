# Plan: DataWarehouse Proyecto 1 - Compras y Ventas

## Descripción General
Construcción de un DataWarehouse integrado con dos bases de datos operacionales separadas que consolidan datos de compras y ventas. Utiliza Docker para SQL Server 2019, archivos de texto delimitados por "|" (.comp y .vent), SSIS para ETL (limpieza y carga dimensional), y SSAS para crear cubos analíticos.

**Duración:** 3 horas 15 minutos  
**Fecha:** 8 de Abril, 2026  
**Plataforma:** Microsoft SQL Server 2019 (Docker)

---

## Requisitos Técnicos Aplicados

- ✓ Herramientas: Microsoft Visual Studio (SSIS + SSAS)
- ✓ Plataforma: SQL Server + SQL Server Analysis Services
- ✓ Fuentes heterogéneas: Archivos delimitados por "|" (.comp, .vent)
- ✓ Procesos ETL: Extracción → Transformación → Limpieza → Carga → Validación
- ✓ Modelo: Star Schema en SQL Server + SSAS multidimensional
- ✓ Conocimientos: ETL, Data Warehouse, Modelado dimensional, SSAS

---

## Arquitectura General  

```
Fuentes Heterogéneas
├─ compras.comp (delimitado |) → SSIS Package 1 (ETL Compras)
└─ ventas.vent   (delimitado |) → SSIS Package 2 (ETL Ventas)
         ↓
Docker Container - SQL Server 2019
├─ BD Compras_DB
│  ├─ Staging (stg_Compras)
│  ├─ Dimensiones (Dim_Fecha, Dim_Producto, Dim_Proveedor, Dim_Sucursal)
│  └─ Hechos (Fact_Compras)
│
├─ BD Ventas_DB
│  ├─ Staging (stg_Ventas)
│  ├─ Dimensiones (Dim_Fecha, Dim_Producto, Dim_Cliente, Dim_Vendedor, Dim_Sucursal)
│  └─ Hechos (Fact_Ventas)
│
└─ BD Central_DW (opcional, para consolidación)
         ↓
SSAS Analysis Services
├─ Cube Compras (Perspectiva Compras)
├─ Cube Ventas (Perspectiva Ventas)
└─ Reportes integrados
```

---

## FASE 1: Preparación Docker & Bases de Datos (30 min)

### 1.1 Crear Instancia Docker SQL Server

**Archivo:** `docker.txt` (ejecutar comandos desde terminal)

Contiene:
- Comando crear contenedor
- Validación de estado
- Configuración de red

### 1.2 Crear Dos Bases de Datos Separadas

**BD 1: Compras_DB**
- Tablas Staging: stg_Compras
- Dimensiones: Dim_Fecha, Dim_Producto, Dim_Proveedor, Dim_Sucursal
- Hechos: Fact_Compras
- Archivo DDL: `01_COMPRAS_DB.ddl`

**BD 2: Ventas_DB**
- Tablas Staging: stg_Ventas
- Dimensiones: Dim_Fecha, Dim_Producto, Dim_Cliente, Dim_Vendedor, Dim_Sucursal
- Hechos: Fact_Ventas
- Archivo DDL: `02_VENTAS_DB.ddl`

**Configuración:**
- Contenedor: Proyecto1 (puerto 1433)
- Usuario: sa
- Contraseña: contra123

---

## FASE 2: Preparación de Datos (15 min)

### 2.1 Estructura de Archivos Origen

**Archivo: compras.comp**
- Delimitador: "|"
- Columnas: Fecha|CodProveedor|NombreProveedor|CodProducto|NombreProducto|MarcaProducto|Categoria|CodSucursal|NombreSucursal|Region|Departamento|Unidades|CostoUnitario
- Encoding: UTF-8

**Archivo: ventas.vent**
- Delimitador: "|"
- Columnas: Fecha|CodCliente|NombreCliente|TipoCliente|CodVendedor|NombreVendedor|CodProducto|NombreProducto|MarcaProducto|Categoria|CodSucursal|NombreSucursal|Region|Departamento|Unidades|PrecioUnitario
- Encoding: UTF-8

### 2.2 Validación de Datos

Conversión desde CSV original:
- Cambiar delimitador de "," a "|"
- Exportar como .comp y .vent respectivamente
- Validar encoding UTF-8

---

## FASE 3: Diseño SSIS ETL Packages (50 min)

### 3.1 Estructura de Paquetes SSIS

**Solución:** Proyecto1_SSIS.sln

#### Paquete 1: SSIS_LimpiarDimensiones_Compras.dtsx

Poblado de dimensiones de Compras_DB:

1. **Dim_Fecha** (Compartida)
   - Rango: 01/01/2018 a 31/12/2024
   - Columnas: SK_Fecha, FechaCompleta, Año, Mes, Trimestre, Día, NombreMes, NombreDía

2. **Dim_Producto** (Compras)
   - Origen: compras.comp → Distinct sobre CodProducto
   - Transformación: Categoría "Ninguna" → "SIN CATEGORÍA"

3. **Dim_Proveedor**
   - Origen: compras.comp → Deduplicar por CodProveedor + NombreProveedor

4. **Dim_Sucursal** (Compartida)
   - Origen: compras.comp + ventas.vent → Deduplicar

#### Paquete 2: SSIS_LimpiarDimensiones_Ventas.dtsx

Poblado de dimensiones de Ventas_DB:

1. **Dim_Producto** (Ventas)
   - Origen: ventas.vent → Distinct

2. **Dim_Cliente**
   - Origen: ventas.vent → Deduplicar por CodCliente

3. **Dim_Vendedor**
   - Origen: ventas.vent → Deduplicar por CodVendedor

#### Paquete 3: SSIS_ComprasETL.dtsx

**Flujo de Datos:**
1. **Origen:** Flat File Delimitado (compras.comp, delimitador "|")
2. **Transformaciones:**
   - Data Conversion: Fecha (text) → DATETIME válido
   - Manejo: "Z3/08/2018", "03/08/Z018" → NULL + log de errores
   - Conditional Split: Filtrar registros con Fecha NULL
   - Derived Column:
     * Categoría = IIF(Categoría == "Ninguna", "SIN CATEGORÍA", Categoría)
     * MontoTotal = Unidades * CostoUnitario
   - Lookup Producto: CodProducto → SK_Producto (conexión Compras_DB)
   - Lookup Proveedor: CodProveedor → SK_Proveedor
   - Lookup Sucursal: CodSucursal → SK_Sucursal
   - Lookup Fecha: Fecha → SK_Fecha
3. **Destino:** Fact_Compras (BD Compras_DB)

#### Paquete 4: SSIS_VentasETL.dtsx

**Flujo de Datos:** Estructura similar a ComprasETL

1. **Origen:** Flat File Delimitado (ventas.vent, delimitador "|")
2. **Transformaciones:** Idénticas a ComprasETL
   - Lookups adicionales: Cliente, Vendedor (conexión Ventas_DB)
3. **Destino:** Fact_Ventas (BD Ventas_DB)

### 3.2 Orden de Ejecución SSIS

```
1. SSIS_LimpiarDimensiones_Compras.dtsx
2. SSIS_LimpiarDimensiones_Ventas.dtsx
3. SSIS_ComprasETL.dtsx + SSIS_VentasETL.dtsx (paralelo)
```

### 3.3 Manejo de Datos Limpios

| Situación | Decisión |
|-----------|----------|
| Fecha malformada | DATETIME válido si es posible; NULL + log si no |
| Unidades negativas | Permitidas (devoluciones) |
| Categoría "Ninguna" | Reemplazar por "SIN CATEGORÍA" |
| Duplicados dimensiones | Deduplicar por código + nombre |

---

## FASE 4: Creación SSAS Cubos Analíticos (30 min)

### 4.1 Proyecto SSAS en Visual Studio

**Solución:** Proyecto1_SSAS.sln

**Data Sources:** Dos conexiones
- Data Source Compras: Compras_DB (localhost, 1433, sa/contra123)
- Data Source Ventas: Ventas_DB (localhost, 1433, sa/contra123)

### 4.2 Cube 1: Compras_Cube (BD Compras_DB)

#### Medidas:
- Unidades Compradas (SUM)
- Costo Total Compras (SUM)
- Costo Unitario Promedio (AVERAGE)

#### Dimensiones:
- **Dim_Producto** (Jerarquía: Categoría → Marca → Producto)
- **Dim_Fecha** (Jerarquía: Año → Trimestre → Mes → Día)
- **Dim_Sucursal** (Jerarquía: Región → Departamento → Sucursal)
- **Dim_Proveedor** (flat)

#### Perspectiva: Perspectiva_Compras

### 4.3 Cube 2: Ventas_Cube (BD Ventas_DB)

#### Medidas:
- Unidades Vendidas (SUM)
- Ingresos Totales (SUM)
- Precio Unitario Promedio (AVERAGE)

#### Medidas Calculadas:
- **Margen Bruto** = (Ingresos - Costo Compras) / Ingresos * 100 (referencia desde Compras_Cube)
- **Ratio Conversión** = Unidades Vendidas / Unidades Compradas * 100

#### Dimensiones:
- **Dim_Producto** (Jerarquía: Categoría → Marca → Producto)
- **Dim_Fecha** (Jerarquía: Año → Trimestre → Mes → Día)
- **Dim_Sucursal** (Jerarquía: Región → Departamento → Sucursal)
- **Dim_Cliente** (flat)
- **Dim_Vendedor** (flat)

#### Perspectiva: Perspectiva_Ventas

### 4.4 Deployment SSAS

```
Build Solution → Deploy Compras_Cube → Deploy Ventas_Cube 
→ Process Cubes → Verificar en Analysis Services
```

---

## FASE 5: Validación & QA (20 min)

### 5.1 Validaciones SQL

**En Compras_DB:**
```sql
SELECT 'Compras' tabla, COUNT(*) registros FROM Fact_Compras;
SELECT s.NombreSucursal, SUM(fc.MontoTotal) total_compras
FROM Fact_Compras fc
JOIN Dim_Sucursal s ON fc.SK_Sucursal = s.SK_Sucursal
GROUP BY s.NombreSucursal
ORDER BY total_compras DESC;
```

**En Ventas_DB:**
```sql
SELECT 'Ventas' tabla, COUNT(*) registros FROM Fact_Ventas;
SELECT c.NombreCliente, SUM(fv.MontoTotal) total_ventas
FROM Fact_Ventas fv
JOIN Dim_Cliente c ON fv.SK_Cliente = c.SK_Cliente
GROUP BY c.NombreCliente
ORDER BY total_ventas DESC;
```

### 5.2 Validaciones SSAS

- Conectar MDX queries a ambos cubos
- Verificar jerarquías y dimensiones
- Validar medidas calculadas

---

## FASE 6: Documentación (20 min)

### 6.1 Archivos Generados

```
Proyecto_1/
├─ Docker/
│  └─ docker.txt (comandos para iniciar contenedor)
│
├─ SQL/
│  ├─ 01_COMPRAS_DB.ddl (Staging + Star Schema Compras)
│  ├─ 02_VENTAS_DB.ddl (Staging + Star Schema Ventas)
│  └─ 03_VALIDACION_QUERIES.sql (QA queries)
│
├─ SSIS/
│  ├─ Proyecto1_SSIS.sln
│  ├─ SSIS_LimpiarDimensiones_Compras.dtsx
│  ├─ SSIS_LimpiarDimensiones_Ventas.dtsx
│  ├─ SSIS_ComprasETL.dtsx
│  └─ SSIS_VentasETL.dtsx
│
├─ SSAS/
│  ├─ Proyecto1_SSAS.sln
│  ├─ Compras_Cube.cube
│  └─ Ventas_Cube.cube
│
├─ Datos_Origen/
│  ├─ compras.comp (delimitado por |)
│  └─ ventas.vent (delimitado por |)
│
├─ Plan.md (este archivo)
└─ README.md (documentación final)
```

---

## Modelo de Datos - Star Schema

### BD Compras_DB

**Staging:**
- stg_Compras (Datos crudos desde compras.comp)

**Dimensiones:**
- Dim_Fecha (SK_Fecha, FechaCompleta, Año, Mes, etc.)
- Dim_Producto (SK_Producto, CodProducto, NombreProducto, MarcaProducto, Categoria)
- Dim_Proveedor (SK_Proveedor, CodProveedor, NombreProveedor)
- Dim_Sucursal (SK_Sucursal, CodSucursal, NombreSucursal, Region, Departamento)

**Hechos:**
- Fact_Compras (SK_Compra, SK_Fecha, SK_Producto, SK_Proveedor, SK_Sucursal, Unidades, CostoUnitario, MontoTotal)

### BD Ventas_DB

**Staging:**
- stg_Ventas (Datos crudos desde ventas.vent)

**Dimensiones:**
- Dim_Fecha (Compartida conceptualmente con Compras_DB)
- Dim_Producto (Específica de ventas)
- Dim_Cliente (SK_Cliente, CodCliente, NombreCliente, TipoCliente)
- Dim_Vendedor (SK_Vendedor, CodVendedor, NombreVendedor)
- Dim_Sucursal (Específica de ventas)

**Hechos:**
- Fact_Ventas (SK_Venta, SK_Fecha, SK_Producto, SK_Cliente, SK_Vendedor, SK_Sucursal, Unidades, PrecioUnitario, MontoTotal)

---

## Decisiones de Diseño

| Decisión | Por qué |
|----------|---------|
| **Dos BDs separadas** | Separación de preocupaciones, autonomía, posible distribución futura |
| **Star Schema** | Simplicidad + rendimiento para análisis OLAP |
| **SurrogateKeys numéricos** | Independencia operacional + mejor performance |
| **SSIS con delimitador \|** | Integración con fuentes heterogéneas |
| **Docker SQL Server** | Portabilidad, fácil destrucción/reconstrucción |
| **SSAS Multidimensional** | Madurez, jerarquías, perspectivas |
| **SCD Type 1** | Sobrescribir valores antiguos, suficiente |
| **Perspectivas separadas** | Análisis diferenciado por negocio |

---

## Checklist de Implementación

### Docker & BDs
- [ ] Contenedor Docker corriendo
- [ ] BD Compras_DB creada
- [ ] BD Ventas_DB creada
- [ ] Tablas staging existentes

### Dimensiones
- [ ] Dim_Fecha poblada (2018-2024)
- [ ] Dim_Producto_Compras poblada
- [ ] Dim_Producto_Ventas poblada
- [ ] Dim_Proveedor poblada
- [ ] Dim_Cliente poblada
- [ ] Dim_Vendedor poblada
- [ ] Dim_Sucursal poblada

### SSIS ETL
- [ ] SSIS_LimpiarDimensiones_Compras ejecutado
- [ ] SSIS_LimpiarDimensiones_Ventas ejecutado
- [ ] SSIS_ComprasETL ejecutado sin errores
- [ ] SSIS_VentasETL ejecutado sin errores
- [ ] Fact_Compras poblada (>0 registros)
- [ ] Fact_Ventas poblada (>0 registros)

### SSAS
- [ ] Compras_Cube deployado
- [ ] Ventas_Cube deployado
- [ ] Medidas calculadas funcionando
- [ ] Jerarquías accesibles

### Validación
- [ ] Queries SQL retornan resultados coherentes
- [ ] MDX queries en SSAS funcionan
- [ ] README.md completo

---

## Timeline Estimado

- **00:00-00:30** → Docker setup + BDs creadas
- **00:30-01:00** → Scripts DDL ejecutados
- **01:00-01:40** → SSIS dimensiones
- **01:40-02:20** → SSIS ETL (Compras + Ventas)
- **02:20-02:50** → SSAS cubos
- **02:50-03:15** → Validación + Documentación

**Total: 3 horas 15 minutos**

---

## Enlaces Útiles

### Docker & SQL Server
- [SQL Server en Docker](https://hub.docker.com/_/microsoft-mssql-server)
- [Conectar SSMS a Docker](https://learn.microsoft.com/es-es/sql/linux/quickstart-install-connect-docker)

### SSIS
- [SSIS Flat File Delimitado](https://learn.microsoft.com/es-es/sql/integration-services/import-export-data/flat-file-source)
- [Data Conversion Transformation](https://learn.microsoft.com/es-es/sql/integration-services/data-flow/transformations/data-conversion-transformation)
- [Lookup Transformation](https://learn.microsoft.com/es-es/sql/integration-services/data-flow/transformations/lookup-transformation)

### SSAS
- [Analysis Services Multidimensional](https://learn.microsoft.com/es-es/analysis-services/multidimensional-models/multidimensional-models-ssas)
- [Crear Cubos SSAS](https://learn.microsoft.com/es-es/analysis-services/multidimensional-models/multidimensional-models-ssas)
- [Perspectivas en SSAS](https://learn.microsoft.com/es-es/analysis-services/multidimensional-models-olap-logical-cube-objects/perspectives)

---

**Última actualización:** 8 Abril, 2026  
**Estado:** Plan Aprobado - Listo para Implementación  
**Confirmaciones:** ✓ Docker.txt | ✓ DDL separados | ✓ Dos BDs | ✓ Archivos | ✓ Requisitos técnicos integrados
