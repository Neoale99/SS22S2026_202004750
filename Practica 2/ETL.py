import pandas as pd
import pyodbc
import logging
from datetime import datetime
import os
import sys

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_process.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==================== CONFIGURACIÓN ====================
DATASET1_PATH = 'Dataset 1.csv'
DATASET2_PATH = 'Dataset 2.csv'

DATABASE_CONFIG = {
    'Server': 'localhost,1433',
    'Database': 'Semi2_P1',
    'UID': 'sa',
    'PWD': 'PasswordSegura123!',
    'Driver': '{ODBC Driver 18 for SQL Server}'
}


# ==================== FASE 1: EXTRACCIÓN ====================
class ExtractorCSV:
    @staticmethod
    def extract_data():
        """Extrae datos de los archivos CSV y los merguea."""
        logger.info("====== INICIANDO FASE DE EXTRACCIÓN ======")
        
        try:
            # Leer Dataset 2 (pasajeros)
            if not os.path.exists(DATASET2_PATH):
                logger.error(f"Archivo no encontrado: {DATASET2_PATH}")
                return None
            
            logger.info(f"Extrayendo datos de pasajeros: {DATASET2_PATH}")
            df_passengers = pd.read_csv(DATASET2_PATH, sep=';', encoding='utf-8')
            logger.info(f" Registros extraídos de pasajeros: {len(df_passengers)}")
            
            # Leer Dataset 1 (vuelos)
            if not os.path.exists(DATASET1_PATH):
                logger.error(f"Archivo no encontrado: {DATASET1_PATH}")
                return None
            
            logger.info(f"Extrayendo datos de vuelos: {DATASET1_PATH}")
            df_flights = pd.read_csv(DATASET1_PATH, encoding='utf-8')
            logger.info(f" Registros extraídos de vuelos: {len(df_flights)}")
            
            # Merge circular: asignar un vuelo a cada pasajero
            df_passengers['flight_index'] = [i % len(df_flights) for i in range(len(df_passengers))]
            df_flights_indexed = df_flights.reset_index().rename(columns={'index': 'flight_index'})
            
            df_merged = df_passengers.merge(
                df_flights_indexed,
                left_on='flight_index',
                right_on='flight_index',
                how='left'
            )
            
            df_merged = df_merged.drop_duplicates(subset=['passenger_id', 'booking_datetime'], keep='first')
            logger.info(f" Total de registros después del merge: {len(df_merged)}")
            return df_merged
            
        except Exception as e:
            logger.error(f"Error en extracción: {e}")
            return None


# ==================== FASE 2: TRANSFORMACIÓN ====================
class Transformer:
    @staticmethod
    def clean_gender(gender):
        """Normaliza valores de género."""
        if pd.isna(gender):
            return 'X'
        gender = str(gender).strip().upper()
        if gender in ['M', 'MASCULINO']:
            return 'M'
        elif gender in ['F', 'FEMENINO']:
            return 'F'
        else:
            return 'X'
    
    @staticmethod
    def parse_date(date_str):
        """Parsea diferentes formatos de fecha."""
        if pd.isna(date_str):
            return None
        
        date_str = str(date_str).strip()
        date_formats = [
            '%d/%m/%Y %H:%M',
            '%m-%d-%Y %I:%M %p',
            '%d/%m/%Y %H:%M:%S'
        ]
        
        for fmt in date_formats:
            try:
                return pd.to_datetime(date_str, format=fmt)
            except:
                continue
        
        try:
            return pd.to_datetime(date_str)
        except:
            logger.warning(f"No se pudo parsear fecha: {date_str}")
            return None
    
    @staticmethod
    def clean_price(price_str):
        """Limpia valores de precio (reemplaza comas por puntos)."""
        if pd.isna(price_str):
            return 0.0
        price_str = str(price_str).strip().replace(',', '.')
        try:
            return float(price_str)
        except:
            logger.warning(f"No se pudo convertir precio: {price_str}")
            return 0.0
    
    @staticmethod
    def clean_age(age):
        """Valida y limpia edades."""
        if pd.isna(age):
            return None
        try:
            age_int = int(age)
            if 0 <= age_int <= 120:
                return age_int
            else:
                logger.warning(f"Edad fuera de rango: {age_int}")
                return None
        except:
            logger.warning(f"No se pudo convertir edad: {age}")
            return None
    
    @staticmethod
    def transform_data(df):
        """Aplica transformaciones al dataframe."""
        logger.info("====== INICIANDO FASE DE TRANSFORMACIÓN ======")
        
        try:
            df = df.copy()
            
            # Limpieza de géneros
            logger.info("Limpiando género...")
            df['passenger_gender'] = df['passenger_gender'].apply(Transformer.clean_gender)
            
            # Parseo de fechas
            logger.info("Parseando fechas...")
            df['booking_datetime'] = df['booking_datetime'].apply(Transformer.parse_date)
            df = df.dropna(subset=['booking_datetime'])
            
            # Limpieza de precios
            logger.info("Limpiando precios...")
            df['ticket_price'] = df['ticket_price'].apply(Transformer.clean_price)
            df['ticket_price_usd_est'] = df['ticket_price_usd_est'].apply(Transformer.clean_price)
            
            # Limpieza de edades
            logger.info("Limpiando edades...")
            df['passenger_age'] = df['passenger_age'].apply(Transformer.clean_age)
            df['passenger_age'] = df['passenger_age'].fillna(0)
            
            # Rellenar nacionalidades faltantes
            df['passenger_nationality'] = df['passenger_nationality'].fillna('UNKNOWN')
            
            # Rellenar canales de venta faltantes
            df['sales_channel'] = df['sales_channel'].fillna('OTHER')
            
            # Validar bags
            if 'bags_total' in df.columns:
                df['bags_total'] = pd.to_numeric(df['bags_total'], errors='coerce').fillna(0).astype(int)
            if 'bags_checked' in df.columns:
                df['bags_checked'] = pd.to_numeric(df['bags_checked'], errors='coerce').fillna(0).astype(int)
            
            # Validar campos de vuelos (nombres reales de Dataset 1)
            required_flight_cols = [
                'origin_airport', 'destination_airport', 'aircraft_type', 'airline_code', 'airline_name'
            ]
            for col in required_flight_cols:
                if col not in df.columns:
                    logger.warning(f"Columna {col} no encontrada, llenando con valores por defecto")
                    df[col] = 'UNKNOWN'
            
            logger.info(f" Registros después de transformación: {len(df)}")
            logger.info(" Transformación completada exitosamente")
            return df
            
        except Exception as e:
            logger.error(f"Error en transformación: {e}")
            raise


# ==================== FASE 3: CARGA ====================
class Loader:
    def __init__(self):
        self.connection = None
        self.cursor = None
    
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
            self.cursor = self.connection.cursor()
            logger.info(" Conectado a SQL Server")
            return True
        except Exception as e:
            logger.error(f"Error al conectar a SQL Server: {e}")
            return False
    
    def disconnect(self):
        """Cierra la conexión con SQL Server."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Desconectado de SQL Server")
    
    def insert_pasajeros(self, df):
        """Inserta datos en Dim_Pasajero."""
        logger.info("Insertando pasajeros en Dim_Pasajero...")
        inserted = 0
        
        try:
            for _, row in df.iterrows():
                try:
                    sql = """
                        INSERT INTO Dim_Pasajero (passenger_id, passenger_gender, passenger_age, passenger_nationality)
                        VALUES (?, ?, ?, ?)
                    """
                    self.cursor.execute(sql, (
                        row['passenger_id'],
                        row['passenger_gender'],
                        int(row['passenger_age']),
                        row['passenger_nationality']
                    ))
                    inserted += 1
                except pyodbc.IntegrityError:
                    # Pasajero ya existe, continuar
                    continue
                except Exception as e:
                    logger.warning(f"Error insertando pasajero {row['passenger_id']}: {e}")
                    continue
            
            self.connection.commit()
            logger.info(f" {inserted} pasajeros insertados en Dim_Pasajero")
            return inserted
        except Exception as e:
            logger.error(f"Error en insert_pasajeros: {e}")
            self.connection.rollback()
            return 0
    
    def insert_tiempos(self, df):
        """Inserta datos en Dim_Tiempo."""
        logger.info("Insertando tiempos en Dim_Tiempo...")
        inserted = 0
        
        try:
            unique_dates = df['booking_datetime'].unique()
            for date in unique_dates:
                try:
                    sql = """
                        INSERT INTO Dim_Tiempo (booking_datetime, anio, mes, dia, hora)
                        VALUES (?, ?, ?, ?, ?)
                    """
                    self.cursor.execute(sql, (
                        date,
                        date.year,
                        date.month,
                        date.day,
                        date.hour
                    ))
                    inserted += 1
                except pyodbc.IntegrityError:
                    continue
                except Exception as e:
                    logger.warning(f"Error insertando tiempo {date}: {e}")
                    continue
            
            self.connection.commit()
            logger.info(f" {inserted} tiempos insertados en Dim_Tiempo")
            return inserted
        except Exception as e:
            logger.error(f"Error en insert_tiempos: {e}")
            self.connection.rollback()
            return 0
    
    def insert_canales(self, df):
        """Inserta datos en Dim_CanalVenta."""
        logger.info("Insertando canales en Dim_CanalVenta...")
        inserted = 0
        
        try:
            unique_channels = df['sales_channel'].unique()
            for channel in unique_channels:
                try:
                    sql = "INSERT INTO Dim_CanalVenta (sales_channel) VALUES (?)"
                    self.cursor.execute(sql, (channel,))
                    inserted += 1
                except pyodbc.IntegrityError:
                    continue
                except Exception as e:
                    logger.warning(f"Error insertando canal {channel}: {e}")
                    continue
            
            self.connection.commit()
            logger.info(f" {inserted} canales insertados en Dim_CanalVenta")
            return inserted
        except Exception as e:
            logger.error(f"Error en insert_canales: {e}")
            self.connection.rollback()
            return 0
    
    def insert_metodos_pago(self, df):
        """Inserta datos en Dim_MetodoPago."""
        logger.info("Insertando métodos de pago en Dim_MetodoPago...")
        inserted = 0
        
        try:
            unique_methods = df['payment_method'].unique()
            for method in unique_methods:
                try:
                    sql = "INSERT INTO Dim_MetodoPago (payment_method) VALUES (?)"
                    self.cursor.execute(sql, (method,))
                    inserted += 1
                except pyodbc.IntegrityError:
                    continue
                except Exception as e:
                    logger.warning(f"Error insertando método {method}: {e}")
                    continue
            
            self.connection.commit()
            logger.info(f" {inserted} métodos insertados en Dim_MetodoPago")
            return inserted
        except Exception as e:
            logger.error(f"Error en insert_metodos_pago: {e}")
            self.connection.rollback()
            return 0
    
    def insert_monedas(self, df):
        """Inserta datos en Dim_Moneda."""
        logger.info("Insertando monedas en Dim_Moneda...")
        inserted = 0
        
        try:
            unique_currencies = df['currency'].unique()
            for currency in unique_currencies:
                try:
                    sql = "INSERT INTO Dim_Moneda (currency) VALUES (?)"
                    self.cursor.execute(sql, (currency,))
                    inserted += 1
                except pyodbc.IntegrityError:
                    continue
                except Exception as e:
                    logger.warning(f"Error insertando moneda {currency}: {e}")
                    continue
            
            self.connection.commit()
            logger.info(f" {inserted} monedas insertadas en Dim_Moneda")
            return inserted
        except Exception as e:
            logger.error(f"Error en insert_monedas: {e}")
            self.connection.rollback()
            return 0
    
    def insert_aeropuertos(self, df):
        """Inserta datos en Dim_Aeropuerto."""
        logger.info("Insertando aeropuertos en Dim_Aeropuerto...")
        inserted = 0
        try:
            codigos = set(df['origin_airport'].dropna().unique()).union(set(df['destination_airport'].dropna().unique()))
            for codigo in codigos:
                try:
                    sql = "INSERT INTO Dim_Aeropuerto (airport_code, airport_name) VALUES (?, ?)"
                    self.cursor.execute(sql, (codigo, codigo))
                    inserted += 1
                except pyodbc.IntegrityError:
                    continue
                except Exception as e:
                    logger.warning(f"Error insertando aeropuerto {codigo}: {e}")
                    continue
            self.connection.commit()
            logger.info(f" {inserted} aeropuertos insertados en Dim_Aeropuerto")
            return inserted
        except Exception as e:
            logger.error(f"Error en insert_aeropuertos: {e}")
            self.connection.rollback()
            return 0
    
    def insert_aviones(self, df):
        """Inserta datos en Dim_Avion."""
        logger.info("Insertando aviones en Dim_Avion...")
        inserted = 0
        try:
            tipos = df['aircraft_type'].dropna().unique()
            for tipo in tipos:
                try:
                    sql = "INSERT INTO Dim_Avion (aircraft_type) VALUES (?)"
                    self.cursor.execute(sql, (tipo,))
                    inserted += 1
                except pyodbc.IntegrityError:
                    continue
                except Exception as e:
                    logger.warning(f"Error insertando avion {tipo}: {e}")
                    continue
            self.connection.commit()
            logger.info(f" {inserted} aviones insertados en Dim_Avion")
            return inserted
        except Exception as e:
            logger.error(f"Error en insert_aviones: {e}")
            self.connection.rollback()
            return 0
    
    def insert_aerolineas(self, df):
        """Inserta datos en Dim_Aerolinea."""
        logger.info("Insertando aerolíneas en Dim_Aerolinea...")
        inserted = 0
        try:
            aerolineas = df[['airline_code', 'airline_name']].drop_duplicates()
            for _, row in aerolineas.iterrows():
                try:
                    sql = "INSERT INTO Dim_Aerolinea (airline_code, airline_name) VALUES (?, ?)"
                    self.cursor.execute(sql, (row['airline_code'], row['airline_name']))
                    inserted += 1
                except pyodbc.IntegrityError:
                    continue
                except Exception as e:
                    logger.warning(f"Error insertando aerolínea {row['airline_code']} - {row['airline_name']}: {e}")
                    continue
            self.connection.commit()
            logger.info(f" {inserted} aerolíneas insertadas en Dim_Aerolinea")
            return inserted
        except Exception as e:
            logger.error(f"Error en insert_aerolineas: {e}")
            self.connection.rollback()
            return 0
    
    def get_id_from_dimension(self, table, column, value):
        """Obtiene el ID de una dimensión."""
        try:
            # Mapeo correcto de nombres de tabla a columna ID
            id_column_map = {
                'Dim_Pasajero': 'id_pasajero',
                'Dim_Tiempo': 'id_tiempo',
                'Dim_CanalVenta': 'id_canal',
                'Dim_MetodoPago': 'id_metodo_pago',
                'Dim_Moneda': 'id_moneda',
                'Dim_Aeropuerto': 'id_aeropuerto',
                'Dim_Avion': 'id_avion',
                'Dim_Aerolinea': 'id_aerolinea'
            }
            
            id_column = id_column_map.get(table, f'id_{table.lower().replace("dim_", "")}')
            sql = f"SELECT {id_column} FROM {table} WHERE {column} = ?"
            self.cursor.execute(sql, (value,))
            result = self.cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.warning(f"Error obteniendo ID de {table}: {e}")
            return None
    
    def insert_ventas(self, df):
        """Inserta datos en Hecho_Venta (extendida con vuelos)."""
        logger.info("====== INICIANDO FASE DE CARGA ======")
        logger.info("Insertando ventas en Hecho_Venta...")
        inserted = 0
        
        try:
            for _, row in df.iterrows():
                try:
                    id_pasajero = self.get_id_from_dimension('Dim_Pasajero', 'passenger_id', row['passenger_id'])
                    id_tiempo = self.get_id_from_dimension('Dim_Tiempo', 'booking_datetime', row['booking_datetime'])
                    id_canal = self.get_id_from_dimension('Dim_CanalVenta', 'sales_channel', row['sales_channel'])
                    id_metodo = self.get_id_from_dimension('Dim_MetodoPago', 'payment_method', row['payment_method'])
                    id_moneda = self.get_id_from_dimension('Dim_Moneda', 'currency', row['currency'])
                    id_aero_origen = self.get_id_from_dimension('Dim_Aeropuerto', 'airport_code', row.get('origin_airport', 'UNKNOWN'))
                    id_aero_dest = self.get_id_from_dimension('Dim_Aeropuerto', 'airport_code', row.get('destination_airport', 'UNKNOWN'))
                    id_avion = self.get_id_from_dimension('Dim_Avion', 'aircraft_type', row.get('aircraft_type', 'UNKNOWN'))
                    id_aerolinea = self.get_id_from_dimension('Dim_Aerolinea', 'airline_code', row.get('airline_code', 'UNKNOWN'))
                    
                    if all([id_pasajero, id_tiempo, id_canal, id_metodo, id_moneda, id_aero_origen, id_aero_dest, id_avion, id_aerolinea]):
                        sql = """
                            INSERT INTO Hecho_Venta 
                            (id_pasajero, id_tiempo, id_canal, id_metodo_pago, id_moneda, 
                             id_aeropuerto_origen, id_aeropuerto_destino, id_avion, id_aerolinea,
                             ticket_price, ticket_price_usd_est, bags_total, bags_checked)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
                        self.cursor.execute(sql, (
                            id_pasajero,
                            id_tiempo,
                            id_canal,
                            id_metodo,
                            id_moneda,
                            id_aero_origen,
                            id_aero_dest,
                            id_avion,
                            id_aerolinea,
                            float(row.get('ticket_price', 0)),
                            float(row.get('ticket_price_usd_est', 0)),
                            int(row.get('bags_total', 0)),
                            int(row.get('bags_checked', 0))
                        ))
                        inserted += 1
                    else:
                        logger.warning(f"IDs incompletos para venta {row.get('passenger_id', 'UNKNOWN')}")
                except Exception as e:
                    logger.warning(f"Error insertando venta: {e}")
                    continue
            
            self.connection.commit()
            logger.info(f"{inserted} ventas insertadas en Hecho_Venta")
            logger.info("Carga completada exitosamente")
            return inserted
        except Exception as e:
            logger.error(f"Error en insert_ventas: {e}")
            self.connection.rollback()
            return 0


# ==================== PROCESO ETL ====================
def run_etl():
    try:
        logger.info("\n" + "="*60)
        logger.info("INICIANDO PROCESO ETL")
        logger.info("="*60 + "\n")
        
        # FASE 1: Extracción
        extractor = ExtractorCSV()
        df_raw = extractor.extract_data()
        
        if df_raw is None or df_raw.empty:
            logger.error("No hay datos para procesar")
            return False
        
        # FASE 2: Transformación
        transformer = Transformer()
        df_clean = transformer.transform_data(df_raw)
        
        if df_clean is None or df_clean.empty:
            logger.error("Los datos transformados están vacíos")
            return False
        
        # FASE 3: Carga
        loader = Loader()
        if not loader.connect():
            return False
        
        try:
            # Insertar dimensiones (orden importante para las claves foráneas)
            loader.insert_pasajeros(df_clean)
            loader.insert_tiempos(df_clean)
            loader.insert_canales(df_clean)
            loader.insert_metodos_pago(df_clean)
            loader.insert_monedas(df_clean)
            loader.insert_aeropuertos(df_clean)
            loader.insert_aviones(df_clean)
            loader.insert_aerolineas(df_clean)
            
            # Insertar tabla de hechos
            loader.insert_ventas(df_clean)
            
            logger.info("\n" + "="*60)
            logger.info("PROCESO ETL COMPLETADO EXITOSAMENTE")
            logger.info("="*60 + "\n")
            return True
        finally:
            loader.disconnect()
            
    except Exception as e:
        logger.error(f"Error general en ETL: {e}")
        return False


if __name__ == "__main__":
    success = run_etl()
    sys.exit(0 if success else 1)
