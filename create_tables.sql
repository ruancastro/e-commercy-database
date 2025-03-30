-- Creating the database
CREATE DATABASE Ecommerce_OLTP;
USE Ecommerce_OLTP;

-- Categories Table
CREATE TABLE Categories (
    id INT NOT NULL AUTO_INCREMENT,
    name VARCHAR(50) NOT NULL,
    PRIMARY KEY (id)
);

-- Items Table
CREATE TABLE Item (
    id INT NOT NULL AUTO_INCREMENT,
    name VARCHAR(50) NOT NULL,
    category_id INT NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (category_id) REFERENCES Categories(id)
);

-- Sizes Table
CREATE TABLE Sizes (
    id INT NOT NULL AUTO_INCREMENT,
    size VARCHAR(4) NOT NULL,
    PRIMARY KEY (id)
);

-- Prices Table
CREATE TABLE Price (
    item_id INT NOT NULL,
    size_id INT NOT NULL,
    value FLOAT NOT NULL CHECK (value > 0), -- Garante que o preço seja positivo
    PRIMARY KEY (item_id, size_id),
    FOREIGN KEY (item_id) REFERENCES Item(id),
    FOREIGN KEY (size_id) REFERENCES Sizes(id)
);

-- Stores Table
CREATE TABLE Stores (
    id INT NOT NULL AUTO_INCREMENT,
    name VARCHAR(50) NOT NULL,
    PRIMARY KEY (id)
);

-- Addresses Table
CREATE TABLE Addresses (
    id INT NOT NULL AUTO_INCREMENT,
    street VARCHAR(100) NOT NULL,
    number VARCHAR(10) NOT NULL,
    complement VARCHAR(50),
    neighborhood VARCHAR(50),
    city VARCHAR(50),
    state CHAR(2) NOT NULL CHECK (state IN ('AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO')),
    zip_code VARCHAR(10) NOT NULL CHECK (zip_code REGEXP '^[0-9]{5}(-?[0-9]{3})?$'),
    country VARCHAR(50) DEFAULT 'Brasil',
    PRIMARY KEY (id)
);

-- Store Addresses Table
CREATE TABLE Store_address (
    store_id INT NOT NULL,
    address_id INT NOT NULL,
    PRIMARY KEY (store_id, address_id),
    FOREIGN KEY (store_id) REFERENCES Stores(id) ON DELETE CASCADE,
    FOREIGN KEY (address_id) REFERENCES Addresses(id) ON DELETE CASCADE
);

-- Customers Table
CREATE TABLE Customers (
    customer_id INT NOT NULL AUTO_INCREMENT,
    full_name VARCHAR(50) NOT NULL,
    email VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (customer_id)
);

-- Customer Addresses Table
CREATE TABLE Customer_addresses (
    customer_id INT NOT NULL,
    address_id INT NOT NULL,
    PRIMARY KEY (customer_id, address_id),
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id) ON DELETE CASCADE,
    FOREIGN KEY (address_id) REFERENCES Addresses(id) ON DELETE CASCADE
);

-- Phones Table
CREATE TABLE Phones (
    id INT NOT NULL AUTO_INCREMENT,
    phone_type VARCHAR(20),
    number VARCHAR(20) NOT NULL CHECK (number REGEXP '^[0-9]+$'), -- Only numbers
    PRIMARY KEY (id)
);

-- Customer Phones Table
CREATE TABLE Phones_customers (
    phone_id INT NOT NULL,
    customer_id INT NOT NULL,
    PRIMARY KEY (phone_id, customer_id),
    FOREIGN KEY (phone_id) REFERENCES Phones(id) ON DELETE CASCADE,
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id) ON DELETE CASCADE
);

-- Store Phones Table
CREATE TABLE Phones_store (
    phone_id INT NOT NULL,
    store_id INT NOT NULL,
    PRIMARY KEY (phone_id, store_id),
    FOREIGN KEY (phone_id) REFERENCES Phones(id) ON DELETE CASCADE,
    FOREIGN KEY (store_id) REFERENCES Stores(id) ON DELETE CASCADE
);

-- Purchases Table
CREATE TABLE Purchase (
    id INT NOT NULL AUTO_INCREMENT,
    customer_id INT NOT NULL,
    item_id INT NOT NULL,
    size_id INT NOT NULL,
    store_id INT NOT NULL,
    order_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id),
    FOREIGN KEY (item_id, size_id) REFERENCES Price(item_id, size_id),
    FOREIGN KEY (store_id) REFERENCES Stores(id)
);

-- Purchase Status Table
CREATE TABLE Purchase_status (
    purchase_id INT NOT NULL,
    status VARCHAR(20) NOT NULL,
    PRIMARY KEY (purchase_id),
    FOREIGN KEY (purchase_id) REFERENCES Purchase(id)
);

-- Inventory Table
CREATE TABLE Inventory (
    item_id INT NOT NULL,
    size_id INT NOT NULL,
    store_id INT NOT NULL,
    PRIMARY KEY (item_id, size_id, store_id),
    FOREIGN KEY (item_id) REFERENCES Item(id),
    FOREIGN KEY (size_id) REFERENCES Sizes(id),
    FOREIGN KEY (store_id) REFERENCES Stores(id)
);
