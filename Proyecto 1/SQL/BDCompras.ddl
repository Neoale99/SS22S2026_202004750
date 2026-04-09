/* =========================================================================
   SCRIPT 01: Crear Base de Datos COMPRAS_DB - Star Schema
   Proyecto: Proyecto_1_DW
   Descripción: Crea tablas staging, dimensiones y hechos para compras
   Fecha: 8 de Abril, 2026
   ========================================================================= */

USE master;
GO

-- Crear base de datos si no existe
IF DB_ID('Compras_DB') IS NULL
BEGIN
    CREATE DATABASE Compras_DB;
    PRINT 'Base de datos Compras_DB creada exitosamente.';
END
ELSE
BEGIN
    PRINT 'Base de datos Compras_DB ya existe.';
END
GO

USE Compras_DB;
GO

/* =========================================================================
   TABLA STAGING: stg_Compras
   Almacena datos crudos de compras.comp
   ========================================================================= */
IF OBJECT_ID('stg_Compras', 'U') IS NOT NULL
    DROP TABLE stg_Compras;

CREATE TABLE stg_Compras (
    stg_ComprasID INT PRIMARY KEY IDENTITY(1,1),
    Fecha NVARCHAR(50),
    CodProveedor NVARCHAR(50),
    NombreProveedor NVARCHAR(255),
    CodProducto NVARCHAR(50),
    NombreProducto NVARCHAR(255),
    MarcaProducto NVARCHAR(255),
    Categoria NVARCHAR(100),
    CodSucursal NVARCHAR(50),
    NombreSucursal NVARCHAR(255),
    Region NVARCHAR(100),
    Departamento NVARCHAR(100),
    Unidades INT,
    CostoUnitario DECIMAL(18, 2),
);

CREATE NONCLUSTERED INDEX IX_stg_Compras_CodProveedor ON stg_Compras(CodProveedor);
PRINT 'Tabla stg_Compras creada exitosamente.';
GO

/* =========================================================================
   TABLA LOG: Errores de Transformación
   ========================================================================= */
IF OBJECT_ID('log_Errores', 'U') IS NOT NULL
    DROP TABLE log_Errores;

CREATE TABLE log_Errores (
    ErrorID INT PRIMARY KEY IDENTITY(1,1),
    Tabla NVARCHAR(100),
    TipoError NVARCHAR(100),
    Descripcion NVARCHAR(500),
    Dato NVARCHAR(MAX),
    FechaError DATETIME DEFAULT GETDATE()
);

PRINT 'Tabla log_Errores creada exitosamente.';
GO

/* =========================================================================
   DIMENSIÓN: Dim_Fecha
   Rango: 2018-01-01 a 2024-12-31
   ========================================================================= */
IF OBJECT_ID('Dim_Fecha', 'U') IS NOT NULL
    DROP TABLE Dim_Fecha;

CREATE TABLE Dim_Fecha (
    SK_Fecha INT PRIMARY KEY IDENTITY(1,1),
    FechaCompleta DATE NOT NULL UNIQUE,
    Año SMALLINT NOT NULL,
    NumeroMes TINYINT NOT NULL,
    NombreMes NVARCHAR(20),
    Trimestre TINYINT NOT NULL,
    NumeroDía TINYINT NOT NULL,
    NombreDía NVARCHAR(20),
    EsDíaLaborable BIT DEFAULT 1
);

CREATE NONCLUSTERED INDEX IX_Dim_Fecha_FechaCompleta ON Dim_Fecha(FechaCompleta);
PRINT 'Tabla Dim_Fecha creada.';
GO

/* =========================================================================
   DIMENSIÓN: Dim_Producto (Compras)
   ========================================================================= */
IF OBJECT_ID('Dim_Producto', 'U') IS NOT NULL
    DROP TABLE Dim_Producto;

CREATE TABLE Dim_Producto (
    SK_Producto INT PRIMARY KEY IDENTITY(1,1),
    CodProducto NVARCHAR(50) NOT NULL UNIQUE,
    NombreProducto NVARCHAR(255) NOT NULL,
    MarcaProducto NVARCHAR(255),
    Categoria NVARCHAR(100),
    EsActivo BIT DEFAULT 1,
    FechaCreacion DATETIME DEFAULT GETDATE()
);

CREATE NONCLUSTERED INDEX IX_Dim_Producto_CodProducto ON Dim_Producto(CodProducto);
PRINT 'Tabla Dim_Producto creada.';
GO

/* =========================================================================
   DIMENSIÓN: Dim_Proveedor
   ========================================================================= */
IF OBJECT_ID('Dim_Proveedor', 'U') IS NOT NULL
    DROP TABLE Dim_Proveedor;

CREATE TABLE Dim_Proveedor (
    SK_Proveedor INT PRIMARY KEY IDENTITY(1,1),
    CodProveedor NVARCHAR(50) NOT NULL UNIQUE,
    NombreProveedor NVARCHAR(255) NOT NULL,
    EsActivo BIT DEFAULT 1,
    FechaCreacion DATETIME DEFAULT GETDATE()
);

CREATE NONCLUSTERED INDEX IX_Dim_Proveedor_CodProveedor ON Dim_Proveedor(CodProveedor);
PRINT 'Tabla Dim_Proveedor creada.';
GO

/* =========================================================================
   DIMENSIÓN: Dim_Sucursal (Compras)
   ========================================================================= */
IF OBJECT_ID('Dim_Sucursal', 'U') IS NOT NULL
    DROP TABLE Dim_Sucursal;

CREATE TABLE Dim_Sucursal (
    SK_Sucursal INT PRIMARY KEY IDENTITY(1,1),
    CodSucursal NVARCHAR(50) NOT NULL UNIQUE,
    NombreSucursal NVARCHAR(255) NOT NULL,
    Region NVARCHAR(100),
    Departamento NVARCHAR(100),
    EsActivo BIT DEFAULT 1,
    FechaCreacion DATETIME DEFAULT GETDATE()
);

CREATE NONCLUSTERED INDEX IX_Dim_Sucursal_CodSucursal ON Dim_Sucursal(CodSucursal);
PRINT 'Tabla Dim_Sucursal creada.';
GO

/* =========================================================================
   TABLA DE HECHOS: Fact_Compras
   Grano: Una fila por compra por día
   ========================================================================= */
IF OBJECT_ID('Fact_Compras', 'U') IS NOT NULL
    DROP TABLE Fact_Compras;

CREATE TABLE Fact_Compras (
    SK_Compra INT PRIMARY KEY IDENTITY(1,1),
    SK_Fecha INT NOT NULL,
    SK_Producto INT NOT NULL,
    SK_Proveedor INT NOT NULL,
    SK_Sucursal INT NOT NULL,
    Unidades INT NOT NULL,
    CostoUnitario DECIMAL(18, 2) NOT NULL,
    MontoTotal DECIMAL(18, 2) NOT NULL,
    FechaCompra DATETIME DEFAULT GETDATE(),
    CONSTRAINT FK_Fact_Compras_Fecha FOREIGN KEY (SK_Fecha) REFERENCES Dim_Fecha(SK_Fecha),
    CONSTRAINT FK_Fact_Compras_Producto FOREIGN KEY (SK_Producto) REFERENCES Dim_Producto(SK_Producto),
    CONSTRAINT FK_Fact_Compras_Proveedor FOREIGN KEY (SK_Proveedor) REFERENCES Dim_Proveedor(SK_Proveedor),
    CONSTRAINT FK_Fact_Compras_Sucursal FOREIGN KEY (SK_Sucursal) REFERENCES Dim_Sucursal(SK_Sucursal)
);

CREATE NONCLUSTERED INDEX IX_Fact_Compras_SK_Fecha ON Fact_Compras(SK_Fecha);
CREATE NONCLUSTERED INDEX IX_Fact_Compras_SK_Producto ON Fact_Compras(SK_Producto);
CREATE NONCLUSTERED INDEX IX_Fact_Compras_SK_Proveedor ON Fact_Compras(SK_Proveedor);
CREATE NONCLUSTERED INDEX IX_Fact_Compras_SK_Sucursal ON Fact_Compras(SK_Sucursal);

PRINT 'Tabla Fact_Compras creada.';
GO

/* =========================================================================
   POBLAR Dim_Fecha (2018-01-01 a 2024-12-31)
   ========================================================================= */
DECLARE @FechaInicio DATE = '2018-01-01';
DECLARE @FechaFin DATE = '2024-12-31';
DECLARE @FechaActual DATE = @FechaInicio;

WHILE @FechaActual <= @FechaFin
BEGIN
    INSERT INTO Dim_Fecha (FechaCompleta, Año, NumeroMes, NombreMes, Trimestre, NumeroDía, NombreDía)
    VALUES (
        @FechaActual,
        YEAR(@FechaActual),
        MONTH(@FechaActual),
        DATENAME(MONTH, @FechaActual),
        CEILING(MONTH(@FechaActual) / 3.0),
        DAY(@FechaActual),
        DATENAME(WEEKDAY, @FechaActual)
    );
    SET @FechaActual = DATEADD(DAY, 1, @FechaActual);
END

PRINT 'Dim_Fecha poblada con fechas 2018-2024.';
GO

/* =========================================================================
   VALIDACIÓN FINAL
   ========================================================================= */
PRINT '';
PRINT '========================================';
PRINT 'COMPRAS_DB - Star Schema creado';
PRINT '========================================';
PRINT '';
PRINT 'Tablas Staging: 1';
PRINT '  - stg_Compras';
PRINT '';
PRINT 'Dimensiones: 4';
PRINT '  - Dim_Fecha (2557 registros: 2018-2024)';
PRINT '  - Dim_Producto';
PRINT '  - Dim_Proveedor';
PRINT '  - Dim_Sucursal';
PRINT '';
PRINT 'Tablas de Hechos: 1';
PRINT '  - Fact_Compras';
PRINT '';
PRINT 'Próximo paso: Ejecutar SSIS_LimpiarDimensiones_Compras.dtsx';
