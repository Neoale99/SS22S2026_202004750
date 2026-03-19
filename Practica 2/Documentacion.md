# Informe Ejecutivo: Modelo BI de Ventas de Boletos Aéreos

## 1. Descripción General del Modelo

El modelo implementado es un **modelo tabular en estrella** (star schema) en Power BI, alimentado desde una base de datos SQL Server. El modelo integra información de vuelos y pasajeros, permitiendo análisis detallados y flexibles para la toma de decisiones estratégicas.

### Estructura del Modelo
- **Tabla de hechos:** `Hecho_Venta` (ventas de boletos, con claves foráneas a todas las dimensiones)
- **Tablas de dimensiones:**
  - `Dim_Pasajero` (información de pasajeros)
  - `Dim_Tiempo` (fechas y jerarquía año-mes-día-hora)
  - `Dim_CanalVenta` (canal de venta)
  - `Dim_MetodoPago` (método de pago)
  - `Dim_Moneda` (moneda de la transacción)
  - `Dim_Aeropuerto` (aeropuertos de origen y destino)
  - `Dim_Avion` (tipo de avión)
  - `Dim_Aerolinea` (aerolínea)

Las relaciones entre tablas permiten analizar las ventas desde múltiples perspectivas: tiempo, canal, aeropuerto, avión, nacionalidad, etc.

## 2. KPIs y Métricas Implementadas

Se han definido los siguientes KPIs y métricas clave para el análisis y visualización:

- **Vuelos por aeropuerto:**
  - Permite identificar los aeropuertos con mayor tráfico de vuelos.
  - Visualización sugerida: gráfico de barras o mapa.

- **Ingresos totales:**
  - Suma de ventas, con opción de visualizar en diferentes monedas.
  - Visualización: gráfica de línea por mes (eje de tiempo).

- **Total de vuelos:**
  - Cantidad total de vuelos realizados (no ventas, sino vuelos únicos).
  - Visualización: tarjeta o indicador simple.

- **Ganancia total vs Meta (KPI semáforo):**
  - Compara los ingresos totales con una meta definida.
  - Visualización: KPI con semáforo (verde/amarillo/rojo según cumplimiento).

- **Tipos de aviones:**
  - Distribución de ventas por tipo de avión.
  - Visualización: gráfica de pie.

- **Nacionalidad de los viajeros:**
  - Permite analizar la diversidad de pasajeros.
  - Visualización: gráfico de barras.

- **Ventas según el canal:**
  - Muestra la participación de cada canal de venta.
  - Visualización: gráfica de pie.

## 3. Relevancia Estratégica

- **Optimización de rutas y aeropuertos:** Identificar aeropuertos clave y ajustar operaciones para maximizar ingresos y eficiencia.
- **Seguimiento de metas:** El KPI semáforo permite monitorear el cumplimiento de objetivos de ventas en tiempo real.
- **Análisis de mercado:** Conocer la nacionalidad de los viajeros y los canales más efectivos ayuda a enfocar campañas de marketing y mejorar la experiencia del cliente.
- **Gestión de flota:** Analizar la demanda por tipo de avión apoya decisiones de inversión y mantenimiento.
- **Flexibilidad financiera:** La visualización de ingresos en distintas monedas facilita el análisis financiero internacional.

## 4. Visualizaciones

- Gráfico de barras: Vuelos por aeropuerto
- Gráfica de línea: Ingresos totales por mes (con selector de moneda)
- Tarjeta: Total de vuelos
- KPI semáforo: Ganancia total vs Meta
- Pie chart: Tipos de aviones
- Barras: Nacionalidad de los viajeros
- Pie chart: Ventas por canal

