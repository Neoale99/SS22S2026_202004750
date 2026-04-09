/* =========================================================================
   SCRIPT 03: Validación de Datos - Queries QA/Análisis
   Proyecto: Proyecto_1_DW
   Descripción: Queries para validar carga de datos en ambas BDs
   Fecha: 8 de Abril, 2026
   ========================================================================= */

-- =========================================================================
-- VALIDACIONES EN COMPRAS_DB
-- =========================================================================

USE Compras_DB;
GO

PRINT '';
PRINT '========================================';
PRINT 'VALIDACIONES - COMPRAS_DB';
PRINT '========================================';
GO

-- 1. Conteo de registros en dimensiones y hechos
PRINT '';
PRINT '1. CONTEOS POR TABLA';
SELECT 'Dim_Fecha' tabla, COUNT(*) registros FROM Dim_Fecha
UNION ALL
SELECT 'Dim_Producto', COUNT(*) FROM Dim_Producto
UNION ALL
SELECT 'Dim_Proveedor', COUNT(*) FROM Dim_Proveedor
UNION ALL
SELECT 'Dim_Sucursal', COUNT(*) FROM Dim_Sucursal
UNION ALL
SELECT 'Fact_Compras', COUNT(*) FROM Fact_Compras
UNION ALL
SELECT 'stg_Compras', COUNT(*) FROM stg_Compras
ORDER BY tabla;

-- 2. Compras totales por sucursal
PRINT '';
PRINT '2. COMPRAS TOTALES POR SUCURSAL';
SELECT 
    s.NombreSucursal,
    s.Region,
    COUNT(*) num_compras,
    SUM(fc.Unidades) total_unidades,
    SUM(fc.MontoTotal) total_costo
FROM Fact_Compras fc
JOIN Dim_Sucursal s ON fc.SK_Sucursal = s.SK_Sucursal
GROUP BY s.NombreSucursal, s.Region
ORDER BY total_costo DESC;

-- 3. Top 10 productos más comprados
PRINT '';
PRINT '3. TOP 10 PRODUCTOS MAS COMPRADOS';
SELECT TOP 10
    p.NombreProducto,
    p.Categoria,
    p.MarcaProducto,
    COUNT(*) num_compras,
    SUM(fc.Unidades) total_unidades,
    AVG(fc.CostoUnitario) costo_promedio,
    SUM(fc.MontoTotal) costo_total
FROM Fact_Compras fc
JOIN Dim_Producto p ON fc.SK_Producto = p.SK_Producto
GROUP BY p.NombreProducto, p.Categoria, p.MarcaProducto
ORDER BY total_unidades DESC;

-- 4. Compras por proveedor
PRINT '';
PRINT '4. COMPRAS POR PROVEEDOR';
SELECT 
    pr.NombreProveedor,
    COUNT(*) num_compras,
    SUM(fc.Unidades) total_unidades,
    SUM(fc.MontoTotal) total_inversión,
    AVG(fc.MontoTotal) promedio_compra
FROM Fact_Compras fc
JOIN Dim_Proveedor pr ON fc.SK_Proveedor = pr.SK_Proveedor
GROUP BY pr.NombreProveedor
ORDER BY total_inversión DESC;

-- 5. Compras por categoría
PRINT '';
PRINT '5. COMPRAS POR CATEGORIA';
SELECT 
    p.Categoria,
    COUNT(*) num_compras,
    SUM(fc.Unidades) total_unidades,
    MIN(fc.CostoUnitario) costo_min,
    MAX(fc.CostoUnitario) costo_max,
    AVG(fc.CostoUnitario) costo_promedio,
    SUM(fc.MontoTotal) costo_total
FROM Fact_Compras fc
JOIN Dim_Producto p ON fc.SK_Producto = p.SK_Producto
GROUP BY p.Categoria
ORDER BY costo_total DESC;

-- 6. Compras por año/mes (Time Series)
PRINT '';
PRINT '6. COMPRAS POR AÑO-MES (Series Temporal)';
SELECT 
    df.Año,
    df.NombreMes,
    COUNT(*) num_compras,
    SUM(fc.Unidades) total_unidades,
    SUM(fc.MontoTotal) total_costo
FROM Fact_Compras fc
JOIN Dim_Fecha df ON fc.SK_Fecha = df.SK_Fecha
GROUP BY df.Año, df.NombreMes, df.NumeroMes
ORDER BY df.Año, df.NumeroMes;

-- 7. Validar integridad referencial
PRINT '';
PRINT '7. VALIDACION INTEGRIDAD REFERENCIAL';
DECLARE @OrfanosCompras INT;
SELECT @OrfanosCompras = COUNT(*) FROM Fact_Compras fc
WHERE NOT EXISTS (SELECT 1 FROM Dim_Fecha WHERE SK_Fecha = fc.SK_Fecha)
   OR NOT EXISTS (SELECT 1 FROM Dim_Producto WHERE SK_Producto = fc.SK_Producto)
   OR NOT EXISTS (SELECT 1 FROM Dim_Proveedor WHERE SK_Proveedor = fc.SK_Proveedor)
   OR NOT EXISTS (SELECT 1 FROM Dim_Sucursal WHERE SK_Sucursal = fc.SK_Sucursal);

IF @OrfanosCompras = 0
    PRINT '✓ Sin registros huérfanos en Fact_Compras'
ELSE
    PRINT '✗ ALERTA: ' + CAST(@OrfanosCompras AS NVARCHAR(10)) + ' registros huérfanos encontrados';

-- 8. Errores de transformación
PRINT '';
PRINT '8. ERRORES DE TRANSFORMACION';
SELECT 
    Tabla,
    TipoError,
    COUNT(*) cantidad,
    MAX(FechaError) ultima_ocurrencia
FROM log_Errores
GROUP BY Tabla, TipoError
ORDER BY cantidad DESC;

-- =========================================================================
-- VALIDACIONES EN VENTAS_DB
-- =========================================================================

USE Ventas_DB;
GO

PRINT '';
PRINT '========================================';
PRINT 'VALIDACIONES - VENTAS_DB';
PRINT '========================================';
GO

-- 9. Conteo de registros en dimensiones y hechos
PRINT '';
PRINT '9. CONTEOS POR TABLA';
SELECT 'Dim_Fecha' tabla, COUNT(*) registros FROM Dim_Fecha
UNION ALL
SELECT 'Dim_Producto', COUNT(*) FROM Dim_Producto
UNION ALL
SELECT 'Dim_Cliente', COUNT(*) FROM Dim_Cliente
UNION ALL
SELECT 'Dim_Vendedor', COUNT(*) FROM Dim_Vendedor
UNION ALL
SELECT 'Dim_Sucursal', COUNT(*) FROM Dim_Sucursal
UNION ALL
SELECT 'Fact_Ventas', COUNT(*) FROM Fact_Ventas
UNION ALL
SELECT 'stg_Ventas', COUNT(*) FROM stg_Ventas
ORDER BY tabla;

-- 10. Ventas totales por cliente
PRINT '';
PRINT '10. VENTAS TOTALES POR CLIENTE';
SELECT TOP 20
    c.NombreCliente,
    c.TipoCliente,
    COUNT(*) num_ventas,
    SUM(fv.Unidades) total_unidades,
    SUM(fv.MontoTotal) total_ventas
FROM Fact_Ventas fv
JOIN Dim_Cliente c ON fv.SK_Cliente = c.SK_Cliente
GROUP BY c.NombreCliente, c.TipoCliente
ORDER BY total_ventas DESC;

-- 11. Top 10 productos más vendidos
PRINT '';
PRINT '11. TOP 10 PRODUCTOS MAS VENDIDOS';
SELECT TOP 10
    p.NombreProducto,
    p.Categoria,
    p.MarcaProducto,
    COUNT(*) num_ventas,
    SUM(fv.Unidades) total_unidades,
    AVG(fv.PrecioUnitario) precio_promedio,
    SUM(fv.MontoTotal) ingresos_total
FROM Fact_Ventas fv
JOIN Dim_Producto p ON fv.SK_Producto = p.SK_Producto
GROUP BY p.NombreProducto, p.Categoria, p.MarcaProducto
ORDER BY total_unidades DESC;

-- 12. Ventas por vendedor
PRINT '';
PRINT '12. VENTAS POR VENDEDOR';
SELECT 
    v.NombreVendedor,
    COUNT(*) num_ventas,
    SUM(fv.Unidades) total_unidades,
    SUM(fv.MontoTotal) total_ventas,
    AVG(fv.MontoTotal) promedio_venta
FROM Fact_Ventas fv
JOIN Dim_Vendedor v ON fv.SK_Vendedor = v.SK_Vendedor
GROUP BY v.NombreVendedor
ORDER BY total_ventas DESC;

-- 13. Ventas por sucursal
PRINT '';
PRINT '13. VENTAS POR SUCURSAL';
SELECT 
    s.NombreSucursal,
    s.Region,
    COUNT(*) num_ventas,
    SUM(fv.Unidades) total_unidades,
    SUM(fv.MontoTotal) total_ventas
FROM Fact_Ventas fv
JOIN Dim_Sucursal s ON fv.SK_Sucursal = s.SK_Sucursal
GROUP BY s.NombreSucursal, s.Region
ORDER BY total_ventas DESC;

-- 14. Ventas por tipo de cliente
PRINT '';
PRINT '14. VENTAS POR TIPO DE CLIENTE';
SELECT 
    c.TipoCliente,
    COUNT(*) num_ventas,
    SUM(fv.Unidades) total_unidades,
    SUM(fv.MontoTotal) total_ventas,
    AVG(fv.MontoTotal) promedio_venta
FROM Fact_Ventas fv
JOIN Dim_Cliente c ON fv.SK_Cliente = c.SK_Cliente
GROUP BY c.TipoCliente
ORDER BY total_ventas DESC;

-- 15. Ventas por año/mes (Time Series)
PRINT '';
PRINT '15. VENTAS POR AÑO-MES (Series Temporal)';
SELECT 
    df.Año,
    df.NombreMes,
    COUNT(*) num_ventas,
    SUM(fv.Unidades) total_unidades,
    SUM(fv.MontoTotal) total_ingresos
FROM Fact_Ventas fv
JOIN Dim_Fecha df ON fv.SK_Fecha = df.SK_Fecha
GROUP BY df.Año, df.NombreMes, df.NumeroMes
ORDER BY df.Año, df.NumeroMes;

-- 16. Validar integridad referencial
PRINT '';
PRINT '16. VALIDACION INTEGRIDAD REFERENCIAL';
DECLARE @OrfanosVentas INT;
SELECT @OrfanosVentas = COUNT(*) FROM Fact_Ventas fv
WHERE NOT EXISTS (SELECT 1 FROM Dim_Fecha WHERE SK_Fecha = fv.SK_Fecha)
   OR NOT EXISTS (SELECT 1 FROM Dim_Producto WHERE SK_Producto = fv.SK_Producto)
   OR NOT EXISTS (SELECT 1 FROM Dim_Cliente WHERE SK_Cliente = fv.SK_Cliente)
   OR NOT EXISTS (SELECT 1 FROM Dim_Vendedor WHERE SK_Vendedor = fv.SK_Vendedor)
   OR NOT EXISTS (SELECT 1 FROM Dim_Sucursal WHERE SK_Sucursal = fv.SK_Sucursal);

IF @OrfanosVentas = 0
    PRINT '✓ Sin registros huérfanos en Fact_Ventas'
ELSE
    PRINT '✗ ALERTA: ' + CAST(@OrfanosVentas AS NVARCHAR(10)) + ' registros huérfanos encontrados';

-- 17. Errores de transformación
PRINT '';
PRINT '17. ERRORES DE TRANSFORMACION';
SELECT 
    Tabla,
    TipoError,
    COUNT(*) cantidad,
    MAX(FechaError) ultima_ocurrencia
FROM log_Errores
GROUP BY Tabla, TipoError
ORDER BY cantidad DESC;

PRINT '';
PRINT '========================================';
PRINT 'VALIDACION COMPLETADA';
PRINT '========================================';
