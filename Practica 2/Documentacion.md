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

Se han definido e implementado las siguientes medidas DAX y KPIs clave para el análisis y visualización en Power BI:

- **Total de ventas en USD:**
  - Suma de los ingresos por ventas en dólares estadounidenses.
  - Permite monitorear el desempeño financiero global.
  - Visualización: tarjeta, KPI o gráfica de línea.

- **Meta de ventas en USD:**
  - Valor objetivo de ventas definido para el periodo.
  - Se utiliza como referencia para el KPI semáforo.
  - Visualización: KPI.

- **Boletos vendidos:**
  - Cantidad total de boletos vendidos.
  - Mide el volumen de ventas.
  - Visualización: tarjeta o indicador.

- **Total de ventas vs meta de ventas (KPI semáforo):**
  - Compara el total de ventas con la meta establecida, mostrando el avance con un semáforo visual.
  - Visualización: KPI con semáforo (verde/amarillo/rojo según cumplimiento).

- **Vuelos por aeropuerto:**
  - Cantidad de vuelos asociados a cada aeropuerto.
  - Permite identificar los aeropuertos con mayor tráfico.
  - Visualización: gráfico de barras o mapa.

- **Ingresos por mes:**
  - Suma de ingresos agrupados por mes, con opción de filtrar por moneda.
  - Permite analizar tendencias temporales y estacionalidad.
  - Visualización: gráfica de línea.

- **Ventas según el género:**
  - Distribución de ventas por género del pasajero.
  - Permite identificar patrones de compra según género.
  - Visualización: gráfico de barras o pie.

- **Viajeros por nacionalidad:**
  - Cantidad de pasajeros por nacionalidad.
  - Ayuda a conocer la diversidad y el alcance internacional.
  - Visualización: gráfico de barras.

- **Ventas por canal:**
  - Cantidad de ventas realizadas por cada canal de venta.
  - Permite identificar los canales más efectivos.
  - Visualización: gráfica de pie o barras.

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

