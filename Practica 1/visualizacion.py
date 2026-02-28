import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import pyodbc
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuración de conexión SQL Server
DATABASE_CONFIG = {
    'Server': 'localhost,1433',
    'Database': 'Semi2_P1',
    'UID': 'sa',
    'PWD': 'PasswordSegura123!',
    'Driver': '{ODBC Driver 18 for SQL Server}'
}

# Estilo de gráficos
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.size'] = 10


class VisualizadorDatos:
    def __init__(self):
        self.connection = None
        self.connect()
    
    def connect(self):
        """Establece conexión con SQL Server."""
        try:
            connection_string = (
                f"Driver={DATABASE_CONFIG['Driver']};"
                f"Server={DATABASE_CONFIG['Server']};"
                f"Database={DATABASE_CONFIG['Database']};"
                f"UID={DATABASE_CONFIG['UID']};"
                f"PWD={DATABASE_CONFIG['PWD']};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=yes"
            )
            self.connection = pyodbc.connect(connection_string)
            logger.info(" Conectado a SQL Server para visualización")
        except Exception as e:
            logger.error(f"Error al conectar: {e}")
            self.connection = None
    
    def ejecutar_query(self, query):
        """Ejecuta una consulta y retorna un DataFrame."""
        try:
            df = pd.read_sql(query, self.connection)
            return df
        except Exception as e:
            logger.error(f"Error ejecutando query: {e}")
            return None
    
    def close(self):
        """Cierra la conexión."""
        if self.connection:
            self.connection.close()
            logger.info("Conexión cerrada")
    
    def graficar_distribucion_genero(self):
        """Gráfico de distribución por género."""
        query = """
        SELECT 
            dp.passenger_gender AS Genero,
            COUNT(*) AS Total
        FROM Hecho_Venta hv
        INNER JOIN Dim_Pasajero dp ON hv.id_pasajero = dp.id_pasajero
        GROUP BY dp.passenger_gender
        """
        df = self.ejecutar_query(query)
        
        if df is not None and not df.empty:
            fig, ax = plt.subplots(figsize=(8, 5))
            colores = {'M': '#3498db', 'F': '#e74c3c', 'X': '#95a5a6'}
            colors = [colores.get(g, '#34495e') for g in df['Genero']]
            
            bars = ax.bar(df['Genero'], df['Total'], color=colors, edgecolor='black', linewidth=1.5)
            ax.set_xlabel('Género', fontsize=12, fontweight='bold')
            ax.set_ylabel('Cantidad de Vuelos', fontsize=12, fontweight='bold')
            ax.set_title('Distribución de Pasajeros por Género', fontsize=14, fontweight='bold')
            
            # Añadir etiquetas en las barras
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{int(height)}', ha='center', va='bottom', fontweight='bold')
            
            plt.tight_layout()
            plt.savefig('01_distribucion_genero.png', dpi=300, bbox_inches='tight')
            logger.info(" Gráfico: Distribución por género")
    
    def graficar_canales_venta(self):
        """Gráfico de canales de venta."""
        query = """
        SELECT TOP 10
            dcv.sales_channel AS Canal,
            COUNT(*) AS Total_Ventas,
            ROUND(SUM(hv.ticket_price_usd_est), 2) AS Ingresos_USD
        FROM Hecho_Venta hv
        INNER JOIN Dim_CanalVenta dcv ON hv.id_canal = dcv.id_canal
        GROUP BY dcv.sales_channel
        ORDER BY Total_Ventas DESC
        """
        df = self.ejecutar_query(query)
        
        if df is not None and not df.empty:
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
            
            # Gráfico de cantidad
            ax1.barh(df['Canal'], df['Total_Ventas'], color='#3498db', edgecolor='black')
            ax1.set_xlabel('Total de Ventas', fontsize=11, fontweight='bold')
            ax1.set_title('Cantidad de Ventas por Canal', fontsize=12, fontweight='bold')
            ax1.invert_yaxis()
            
            # Gráfico de ingresos
            ax2.barh(df['Canal'], df['Ingresos_USD'], color='#2ecc71', edgecolor='black')
            ax2.set_xlabel('Ingresos (USD)', fontsize=11, fontweight='bold')
            ax2.set_title('Ingresos por Canal de Venta', fontsize=12, fontweight='bold')
            ax2.invert_yaxis()
            
            plt.tight_layout()
            plt.savefig('02_canales_venta.png', dpi=300, bbox_inches='tight')
            logger.info(" Gráfico: Canales de venta")
    
    def graficar_metodos_pago(self):
        """Gráfico de métodos de pago."""
        query = """
        SELECT 
            dmp.payment_method AS Metodo,
            COUNT(*) AS Total,
            ROUND(SUM(hv.ticket_price_usd_est), 2) AS Ingresos_USD
        FROM Hecho_Venta hv
        INNER JOIN Dim_MetodoPago dmp ON hv.id_metodo_pago = dmp.id_metodo_pago
        GROUP BY dmp.payment_method
        ORDER BY Total DESC
        """
        df = self.ejecutar_query(query)
        
        if df is not None and not df.empty:
            fig, ax = plt.subplots(figsize=(10, 6))
            
            colors_pie = plt.cm.Set3(range(len(df)))
            wedges, texts, autotexts = ax.pie(df['Total'], labels=df['Metodo'], autopct='%1.1f%%',
                                               colors=colors_pie, startangle=90, textprops={'fontsize': 10})
            
            for autotext in autotexts:
                autotext.set_color('black')
                autotext.set_fontweight('bold')
            
            ax.set_title('Distribución de Métodos de Pago', fontsize=14, fontweight='bold')
            plt.tight_layout()
            plt.savefig('03_metodos_pago.png', dpi=300, bbox_inches='tight')
            logger.info(" Gráfico: Métodos de pago")
    
    def graficar_nacionalidades_top(self):
        """Gráfico de nacionalidades más frecuentes."""
        query = """
        SELECT TOP 10
            dp.passenger_nationality AS Nacionalidad,
            COUNT(*) AS Total,
            ROUND(SUM(hv.ticket_price_usd_est), 2) AS Ingresos_USD
        FROM Hecho_Venta hv
        INNER JOIN Dim_Pasajero dp ON hv.id_pasajero = dp.id_pasajero
        WHERE dp.passenger_nationality != 'UNKNOWN'
        GROUP BY dp.passenger_nationality
        ORDER BY Total DESC
        """
        df = self.ejecutar_query(query)
        
        if df is not None and not df.empty:
            fig, ax = plt.subplots(figsize=(12, 6))
            
            ax.bar(df['Nacionalidad'], df['Total'], color='#9b59b6', edgecolor='black', linewidth=1.2)
            ax.set_xlabel('Nacionalidad', fontsize=11, fontweight='bold')
            ax.set_ylabel('Total de Vuelos', fontsize=11, fontweight='bold')
            ax.set_title('Top 10 Nacionalidades por Cantidad de Vuelos', fontsize=13, fontweight='bold')
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            plt.savefig('04_nacionalidades_top.png', dpi=300, bbox_inches='tight')
            logger.info(" Gráfico: Nacionalidades top 10")
    
    def graficar_rango_edades(self):
        """Gráfico de distribución por rango de edad."""
        query = """
        SELECT 
            CASE 
                WHEN dp.passenger_age BETWEEN 0 AND 18 THEN '0-18'
                WHEN dp.passenger_age BETWEEN 19 AND 30 THEN '19-30'
                WHEN dp.passenger_age BETWEEN 31 AND 45 THEN '31-45'
                WHEN dp.passenger_age BETWEEN 46 AND 60 THEN '46-60'
                ELSE '60+'
            END AS Rango_Edad,
            COUNT(*) AS Total,
            ROUND(AVG(hv.ticket_price_usd_est), 2) AS Precio_Promedio
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
        ORDER BY Rango_Edad
        """
        df = self.ejecutar_query(query)
        
        if df is not None and not df.empty:
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
            
            # Cantidad por rango
            ax1.bar(df['Rango_Edad'], df['Total'], color='#e67e22', edgecolor='black', linewidth=1.2)
            ax1.set_xlabel('Rango de Edad', fontsize=11, fontweight='bold')
            ax1.set_ylabel('Cantidad de Vuelos', fontsize=11, fontweight='bold')
            ax1.set_title('Pasajeros por Rango de Edad', fontsize=12, fontweight='bold')
            plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
            
            # Precio promedio por rango
            ax2.plot(df['Rango_Edad'], df['Precio_Promedio'], marker='o', linewidth=2.5, 
                    markersize=8, color='#1abc9c')
            ax2.fill_between(range(len(df)), df['Precio_Promedio'], alpha=0.3, color='#1abc9c')
            ax2.set_xlabel('Rango de Edad', fontsize=11, fontweight='bold')
            ax2.set_ylabel('Precio Promedio (USD)', fontsize=11, fontweight='bold')
            ax2.set_title('Precio Promedio por Rango de Edad', fontsize=12, fontweight='bold')
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
            
            plt.tight_layout()
            plt.savefig('05_rango_edades.png', dpi=300, bbox_inches='tight')
            logger.info(" Gráfico: Rango de edades")
    
    def graficar_maletas(self):
        """Gráfico de análisis de maletas."""
        query = """
        SELECT 
            CASE WHEN bags_checked > 0 THEN 'Con Maletas' ELSE 'Sin Maletas' END AS Categoria,
            COUNT(*) AS Total
        FROM Hecho_Venta
        GROUP BY CASE WHEN bags_checked > 0 THEN 'Con Maletas' ELSE 'Sin Maletas' END
        """
        df = self.ejecutar_query(query)
        
        if df is not None and not df.empty:
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
            
            # Pie chart
            colors = ['#e74c3c' if cat == 'Con Maletas' else '#3498db' for cat in df['Categoria']]
            wedges, texts, autotexts = ax1.pie(df['Total'], labels=df['Categoria'], autopct='%1.1f%%',
                                               colors=colors, startangle=90, textprops={'fontsize': 11})
            for autotext in autotexts:
                autotext.set_color('white')
                autotext.set_fontweight('bold')
            ax1.set_title('Pasajeros Con/Sin Maletas Facturadas', fontsize=12, fontweight='bold')
            
            # Bar chart
            ax2.bar(df['Categoria'], df['Total'], color=colors, edgecolor='black', linewidth=1.5)
            ax2.set_ylabel('Cantidad de Pasajeros', fontsize=11, fontweight='bold')
            ax2.set_title('Distribución de Maletas Facturadas', fontsize=12, fontweight='bold')
            
            plt.tight_layout()
            plt.savefig('07_analisis_maletas.png', dpi=300, bbox_inches='tight')
            logger.info(" Gráfico: Análisis de maletas")
    
    def graficar_resumen_ejecutivo(self):
        """Gráfico resumen con KPIs principales."""
        query = """
        SELECT 
            COUNT(DISTINCT hv.id_pasajero) AS Pasajeros_Unicos,
            COUNT(*) AS Total_Ventas,
            ROUND(SUM(hv.ticket_price_usd_est), 2) AS Ingresos_USD,
            ROUND(AVG(hv.ticket_price_usd_est), 2) AS Precio_Promedio
        FROM Hecho_Venta hv
        """
        df = self.ejecutar_query(query)
        
        if df is not None and not df.empty:
            fig = plt.figure(figsize=(12, 6))
            ax = fig.add_subplot(111)
            ax.axis('off')
            
            # Datos
            metrics = [
                f"Pasajeros Únicos: {int(df.iloc[0]['Pasajeros_Unicos'])}",
                f"Total de Ventas: {int(df.iloc[0]['Total_Ventas'])}",
                f"Ingresos Totales: ${df.iloc[0]['Ingresos_USD']:,.2f}",
                f"Precio Promedio: ${df.iloc[0]['Precio_Promedio']:,.2f}"
            ]
            
            y_pos = 0.9
            for i, metric in enumerate(metrics):
                color = ['#3498db', '#2ecc71', '#e74c3c', '#f39c12'][i]
                ax.text(0.5, y_pos, metric, fontsize=18, fontweight='bold',
                       ha='center', bbox=dict(boxstyle='round', facecolor=color, alpha=0.3))
                y_pos -= 0.2
            
            ax.set_title('Resumen Ejecutivo - KPIs Principales', fontsize=16, fontweight='bold', pad=20)
            plt.tight_layout()
            plt.savefig('08_resumen_ejecutivo.png', dpi=300, bbox_inches='tight')
            logger.info(" Gráfico: Resumen ejecutivo")
    
    def generar_todos_graficos(self):
        """Genera todos los gráficos."""
        logger.info("\n" + "="*60)
        logger.info("GENERANDO VISUALIZACIONES")
        logger.info("="*60 + "\n")
        
        if self.connection is None:
            logger.error("No hay conexión a la base de datos")
            return
        
        try:
            self.graficar_distribucion_genero()
            self.graficar_canales_venta()
            self.graficar_metodos_pago()
            self.graficar_nacionalidades_top()
            self.graficar_rango_edades()
            self.graficar_maletas()
            self.graficar_resumen_ejecutivo()
            
            logger.info("\n" + "="*60)
            logger.info(" VISUALIZACIONES GENERADAS EXITOSAMENTE")
            logger.info("Archivos PNG guardados en el directorio actual")
            logger.info("="*60 + "\n")
        except Exception as e:
            logger.error(f"Error generando gráficos: {e}")


if __name__ == "__main__":
    visualizador = VisualizadorDatos()
    visualizador.generar_todos_graficos()
    visualizador.close()
