-- Tabla de Dimensión: Pasajero
CREATE TABLE Dim_Pasajero (
    id_pasajero INT IDENTITY(1,1) PRIMARY KEY,
    passenger_id VARCHAR(50) UNIQUE,
    passenger_gender VARCHAR(20),
    passenger_age INT,
    passenger_nationality VARCHAR(10)
);

-- Tabla de Dimensión: Tiempo
CREATE TABLE Dim_Tiempo (
    id_tiempo INT IDENTITY(1,1) PRIMARY KEY,
    booking_datetime DATETIME,
    anio INT,
    mes INT,
    dia INT,
    hora INT
);

-- Tabla de Dimensión: Canal de Venta
CREATE TABLE Dim_CanalVenta (
    id_canal INT IDENTITY(1,1) PRIMARY KEY,
    sales_channel VARCHAR(30)
);

-- Tabla de Dimensión: Método de Pago
CREATE TABLE Dim_MetodoPago (
    id_metodo_pago INT IDENTITY(1,1) PRIMARY KEY,
    payment_method VARCHAR(30)
);

-- Tabla de Dimensión: Moneda
CREATE TABLE Dim_Moneda (
    id_moneda INT IDENTITY(1,1) PRIMARY KEY,
    currency VARCHAR(10)
);

-- Tabla de Dimensión: Aeropuerto
CREATE TABLE Dim_Aeropuerto (
    id_aeropuerto INT IDENTITY(1,1) PRIMARY KEY,
    airport_code VARCHAR(10) UNIQUE,
    airport_name VARCHAR(100)
);

-- Tabla de Dimensión: Avion
CREATE TABLE Dim_Avion (
    id_avion INT IDENTITY(1,1) PRIMARY KEY,
    aircraft_type VARCHAR(50) UNIQUE
);

-- Tabla de Dimensión: Aerolínea
CREATE TABLE Dim_Aerolinea (
    id_aerolinea INT IDENTITY(1,1) PRIMARY KEY,
    airline_code VARCHAR(10),
    airline_name VARCHAR(100),
    CONSTRAINT UQ_Aerolinea UNIQUE (airline_code, airline_name)
);

-- Tabla de Hechos: Venta de Boletos Extendida
CREATE TABLE Hecho_Venta (
    id_venta INT IDENTITY(1,1) PRIMARY KEY,
    id_pasajero INT FOREIGN KEY REFERENCES Dim_Pasajero(id_pasajero),
    id_tiempo INT FOREIGN KEY REFERENCES Dim_Tiempo(id_tiempo),
    id_canal INT FOREIGN KEY REFERENCES Dim_CanalVenta(id_canal),
    id_metodo_pago INT FOREIGN KEY REFERENCES Dim_MetodoPago(id_metodo_pago),
    id_moneda INT FOREIGN KEY REFERENCES Dim_Moneda(id_moneda),
    id_aeropuerto_origen INT FOREIGN KEY REFERENCES Dim_Aeropuerto(id_aeropuerto),
    id_aeropuerto_destino INT FOREIGN KEY REFERENCES Dim_Aeropuerto(id_aeropuerto),
    id_avion INT FOREIGN KEY REFERENCES Dim_Avion(id_avion),
    id_aerolinea INT FOREIGN KEY REFERENCES Dim_Aerolinea(id_aerolinea),
    ticket_price DECIMAL(18,2),
    ticket_price_usd_est DECIMAL(18,2),
    bags_total INT,
    bags_checked INT
);