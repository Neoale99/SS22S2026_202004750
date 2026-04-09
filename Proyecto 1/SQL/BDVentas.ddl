/* =========================================================================
   SCRIPT 02: Crear Base de Datos VENTAS_DB - Star Schema
   Proyecto: Proyecto_1_DW
   Descripción: Crea tablas staging, dimensiones y hechos para ventas
   ========================================================================= */

USE master;
GO

-- Crear base de datos si no existe
IF DB_ID('Ventas_DB') IS NULL
BEGIN
    CREATE DATABASE Ventas_DB;
    PRINT 'Base de datos Ventas_DB creada exitosamente.';
END
ELSE
BEGIN
    PRINT 'Base de datos Ventas_DB ya existe.';
END
GO

USE Ventas_DB;
GO

/* =========================================================================
   TABLA STAGING: stg_Ventas
   Almacena datos crudos de ventas.vent
   ========================================================================= */
IF OBJECT_ID('stg_Ventas', 'U') IS NOT NULL
    DROP TABLE stg_Ventas;

CREATE TABLE stg_Ventas (
    stg_VentasID INT PRIMARY KEY IDENTITY(1,1),
    Fecha NVARCHAR(50),
    CodCliente NVARCHAR(50),
    NombreCliente NVARCHAR(255),
    TipoCliente NVARCHAR(50),
    CodVendedor NVARCHAR(50),
    NombreVendedor NVARCHAR(255),
    CodProducto NVARCHAR(50),
    NombreProducto NVARCHAR(255),
    MarcaProducto NVARCHAR(255),
    Categoria NVARCHAR(100),
    CodSucursal NVARCHAR(50),
    NombreSucursal NVARCHAR(255),
    Region NVARCHAR(100),
    Departamento NVARCHAR(100),
    Unidades INT,
    PrecioUnitario DECIMAL(18, 2),
);

CREATE NONCLUSTERED INDEX IX_stg_Ventas_CodCliente ON stg_Ventas(CodCliente);
PRINT 'Tabla stg_Ventas creada exitosamente.';
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
   DIMENSIÓN: Dim_Producto (Ventas)
   ========================================================================= */
IF OBJECT_ID('Dim_Producto', 'U') IS NOT NULL
    DROP TABLE Dim_Producto;

CREATE TABLE Dim_Producto (
    SK_Producto INT PRIMARY KEY IDENTITY(1,1),
    CodProducto NVARCHAR(50) NOT NULL UNIQUE,
    NombreProducto NVARCHAR(255) NOT NULL,
    MarcaProducto NVARCHAR(255),
    Categoria NVARCHAR(100),
    EsActivo BIT DEFAULT 1
);

CREATE NONCLUSTERED INDEX IX_Dim_Producto_CodProducto ON Dim_Producto(CodProducto);
PRINT 'Tabla Dim_Producto creada.';
GO

/* =========================================================================
   DIMENSIÓN: Dim_Cliente
   ========================================================================= */
IF OBJECT_ID('Dim_Cliente', 'U') IS NOT NULL
    DROP TABLE Dim_Cliente;

CREATE TABLE Dim_Cliente (
    SK_Cliente INT PRIMARY KEY IDENTITY(1,1),
    CodCliente NVARCHAR(50) NOT NULL UNIQUE,
    NombreCliente NVARCHAR(255) NOT NULL,
    TipoCliente NVARCHAR(50),
    EsActivo BIT DEFAULT 1
);

CREATE NONCLUSTERED INDEX IX_Dim_Cliente_CodCliente ON Dim_Cliente(CodCliente);
PRINT 'Tabla Dim_Cliente creada.';
GO

/* =========================================================================
   DIMENSIÓN: Dim_Vendedor
   ========================================================================= */
IF OBJECT_ID('Dim_Vendedor', 'U') IS NOT NULL
    DROP TABLE Dim_Vendedor;

CREATE TABLE Dim_Vendedor (
    SK_Vendedor INT PRIMARY KEY IDENTITY(1,1),
    CodVendedor NVARCHAR(50) NOT NULL UNIQUE,
    NombreVendedor NVARCHAR(255) NOT NULL,
    EsActivo BIT DEFAULT 1
); IX_Dim_Vendedor_CodVendedor ON Dim_Vendedor(CodVendedor);
PRINT 'Tabla Dim_Vendedor creada.';
GO

/* =========================================================================
   DIMENSIÓN: Dim_Sucursal (Ventas)
   ========================================================================= */
IF OBJECT_ID('Dim_Sucursal', 'U') IS NOT NULL
    DROP TABLE Dim_Sucursal;

CREATE TABLE Dim_Sucursal (
    SK_Sucursal INT PRIMARY KEY IDENTITY(1,1),
    CodSucursal NVARCHAR(50) NOT NULL UNIQUE,
    NombreSucursal NVARCHAR(255) NOT NULL,
    Region NVARCHAR(100),
    Departamento NVARCHAR(100),
    EsActivo BIT DEFAULT 1
); IX_Dim_Sucursal_CodSucursal ON Dim_Sucursal(CodSucursal);
PRINT 'Tabla Dim_Sucursal creada.';
GO

/* =========================================================================
   TABLA DE HECHOS: Fact_Ventas
   Grano: Una fila por venta por día
   ========================================================================= */
IF OBJECT_ID('Fact_Ventas', 'U') IS NOT NULL
    DROP TABLE Fact_Ventas;

CREATE TABLE Fact_Ventas (
    SK_Venta INT PRIMARY KEY IDENTITY(1,1),
    SK_Fecha INT NOT NULL,
    SK_Producto INT NOT NULL,
    SK_Cliente INT NOT NULL,
    SK_Vendedor INT NOT NULL,
    SK_Sucursal INT NOT NULL,
    Unidades INT NOT NULL,
    PrecioUnitario DECIMAL(18, 2) NOT NULL,
    MontoTotal DECIMAL(18, 2) NOT NULL,
    CONSTRAINT FK_Fact_Ventas_Fecha FOREIGN KEY (SK_Fecha) REFERENCES Dim_Fecha(SK_Fecha),
    CONSTRAINT FK_Fact_Ventas_Producto FOREIGN KEY (SK_Producto) REFERENCES Dim_Producto(SK_Producto),
    CONSTRAINT FK_Fact_Ventas_Cliente FOREIGN KEY (SK_Cliente) REFERENCES Dim_Cliente(SK_Cliente),
    CONSTRAINT FK_Fact_Ventas_Vendedor FOREIGN KEY (SK_Vendedor) REFERENCES Dim_Vendedor(SK_Vendedor),
    CONSTRAINT FK_Fact_Ventas_Sucursal FOREIGN KEY (SK_Sucursal) REFERENCES Dim_Sucursal(SK_Sucursal)
);

CREATE NONCLUSTERED INDEX IX_Fact_Ventas_SK_Fecha ON Fact_Ventas(SK_Fecha);
CREATE NONCLUSTERED INDEX IX_Fact_Ventas_SK_Producto ON Fact_Ventas(SK_Producto);
CREATE NONCLUSTERED INDEX IX_Fact_Ventas_SK_Cliente ON Fact_Ventas(SK_Cliente);
CREATE NONCLUSTERED INDEX IX_Fact_Ventas_SK_Vendedor ON Fact_Ventas(SK_Vendedor);
CREATE NONCLUSTERED INDEX IX_Fact_Ventas_SK_Sucursal ON Fact_Ventas(SK_Sucursal);

PRINT 'Tabla Fact_Ventas creada.';
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
PRINT 'VENTAS_DB - Star Schema creado';
PRINT '========================================';
PRINT '';
PRINT 'Tablas Staging: 1';
PRINT '  - stg_Ventas';
PRINT '';
PRINT 'Dimensiones: 5';
PRINT '  - Dim_Fecha (2557 registros: 2018-2024)';
PRINT '  - Dim_Producto';
PRINT '  - Dim_Cliente';
PRINT '  - Dim_Vendedor';
PRINT '  - Dim_Sucursal';
PRINT '';
PRINT 'Tablas de Hechos: 1';
PRINT '  - Fact_Ventas';
PRINT '';
PRINT 'Próximo paso: Ejecutar SSIS_LimpiarDimensiones_Ventas.dtsx';
