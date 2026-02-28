-- 1. Validar cantidad de registros cargados
SELECT 'Pasajeros' AS Tabla, COUNT(*) AS Total FROM Dim_Pasajero
UNION ALL
SELECT 'Tiempos', COUNT(*) FROM Dim_Tiempo
UNION ALL
SELECT 'Canales', COUNT(*) FROM Dim_CanalVenta
UNION ALL
SELECT 'Métodos Pago', COUNT(*) FROM Dim_MetodoPago
UNION ALL
SELECT 'Monedas', COUNT(*) FROM Dim_Moneda
UNION ALL
SELECT 'Ventas', COUNT(*) FROM Hecho_Venta;

-- 2. Número total de vuelos (ventas)
SELECT COUNT(*) AS Total_Vuelos FROM Hecho_Venta;

-- 3. Distribución por género
SELECT 
    dp.passenger_gender AS Genero,
    COUNT(*) AS Total,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM Hecho_Venta), 2) AS Porcentaje
FROM Hecho_Venta hv
INNER JOIN Dim_Pasajero dp ON hv.id_pasajero = dp.id_pasajero
GROUP BY dp.passenger_gender
ORDER BY Total DESC;

-- 4. Nacionalidades más frecuentes
SELECT TOP 10
    dp.passenger_nationality AS Nacionalidad,
    COUNT(*) AS Total,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM Hecho_Venta), 2) AS Porcentaje
FROM Hecho_Venta hv
INNER JOIN Dim_Pasajero dp ON hv.id_pasajero = dp.id_pasajero
WHERE dp.passenger_nationality != 'UNKNOWN'
GROUP BY dp.passenger_nationality
ORDER BY Total DESC;

-- 5. Canales de venta más utilizados
SELECT 
    dcv.sales_channel AS Canal_Venta,
    COUNT(*) AS Total_Ventas,
    SUM(hv.ticket_price_usd_est) AS Total_Ingresos_USD,
    ROUND(AVG(hv.ticket_price_usd_est), 2) AS Precio_Promedio_USD
FROM Hecho_Venta hv
INNER JOIN Dim_CanalVenta dcv ON hv.id_canal = dcv.id_canal
GROUP BY dcv.sales_channel
ORDER BY Total_Ventas DESC;

-- 6. Métodos de pago más utilizados
SELECT 
    dmp.payment_method AS Metodo_Pago,
    COUNT(*) AS Total_Transacciones,
    SUM(hv.ticket_price_usd_est) AS Total_Ingresos_USD,
    ROUND(AVG(hv.ticket_price_usd_est), 2) AS Precio_Promedio_USD
FROM Hecho_Venta hv
INNER JOIN Dim_MetodoPago dmp ON hv.id_metodo_pago = dmp.id_metodo_pago
GROUP BY dmp.payment_method
ORDER BY Total_Transacciones DESC;


-- 7. Distribución de edad de pasajeros
SELECT 
    CASE 
        WHEN dp.passenger_age BETWEEN 0 AND 18 THEN '0-18'
        WHEN dp.passenger_age BETWEEN 19 AND 30 THEN '19-30'
        WHEN dp.passenger_age BETWEEN 31 AND 45 THEN '31-45'
        WHEN dp.passenger_age BETWEEN 46 AND 60 THEN '46-60'
        ELSE '60+'
    END AS Rango_Edad,
    COUNT(*) AS Total,
    ROUND(AVG(hv.ticket_price_usd_est), 2) AS Precio_Promedio_USD
FROM Hecho_Venta hv
INNER JOIN Dim_Pasajero dp ON hv.id_pasajero = dp.id_pasajero
WHERE dp.passenger_age > 0
GROUP BY 
    CASE 
        WHEN dp.passenger_age BETWEEN 0 AND 18 THEN '0-18'
        WHEN dp.passenger_age BETWEEN 19 AND 30 THEN '19-30'
        WHEN dp.passenger_age BETWEEN 31 AND 45 THEN '31-45'
        WHEN dp.passenger_age BETWEEN 46 AND 60 THEN '46-60'
        ELSE '60+'
    END
ORDER BY Rango_Edad;


-- 8. Análisis de maletas
SELECT 
    'Total de Maletas' AS Metrica,
    SUM(hv.bags_total) AS Cantidad,
    ROUND(AVG(hv.bags_total), 2) AS Promedio
FROM Hecho_Venta hv
UNION ALL
SELECT 
    'Maletas Facturadas',
    SUM(hv.bags_checked),
    ROUND(AVG(hv.bags_checked), 2)
FROM Hecho_Venta hv;



-- 9. Top 10 nacionalidades por ingresos
SELECT TOP 10
    dp.passenger_nationality AS Nacionalidad,
    COUNT(*) AS Total_Ventas,
    ROUND(SUM(hv.ticket_price_usd_est), 2) AS Total_Ingresos_USD,
    ROUND(AVG(hv.ticket_price_usd_est), 2) AS Precio_Promedio_USD
FROM Hecho_Venta hv
INNER JOIN Dim_Pasajero dp ON hv.id_pasajero = dp.id_pasajero
WHERE dp.passenger_nationality != 'UNKNOWN'
GROUP BY dp.passenger_nationality
ORDER BY Total_Ingresos_USD DESC;

-- 10. Estadísticas generales del negocio
SELECT 
    COUNT(DISTINCT hv.id_pasajero) AS Pasajeros_Unicos,
    COUNT(*) AS Total_Ventas,
    ROUND(SUM(hv.ticket_price_usd_est), 2) AS Ingresos_Totales_USD,
    ROUND(AVG(hv.ticket_price_usd_est), 2) AS Precio_Promedio_USD,
    ROUND(MIN(hv.ticket_price_usd_est), 2) AS Precio_Minimo_USD,
    ROUND(MAX(hv.ticket_price_usd_est), 2) AS Precio_Maximo_USD,
    SUM(hv.bags_total) AS Total_Maletas,
    ROUND(AVG(hv.bags_total), 2) AS Promedio_Maletas_Por_Venta
FROM Hecho_Venta hv;
