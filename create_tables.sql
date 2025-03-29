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
    value FLOAT NOT NULL,
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
    number VARCHAR(10),
    complement VARCHAR(50),
    neighborhood VARCHAR(50),
    city VARCHAR(50) NOT NULL,
    state CHAR(2) NOT NULL,
    zip_code VARCHAR(10) NOT NULL,
    country VARCHAR(50) DEFAULT 'Brasil',
    PRIMARY KEY (id)
);

-- Store Addresses Table
CREATE TABLE Store_address (
    store_id INT NOT NULL,
    address_id INT NOT NULL,
    PRIMARY KEY (store_id, address_id),
    FOREIGN KEY (store_id) REFERENCES Stores(id),
    FOREIGN KEY (address_id) REFERENCES Addresses(id)
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
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id),
    FOREIGN KEY (address_id) REFERENCES Addresses(id)
);

-- Phones Table
CREATE TABLE Phones (
    id INT NOT NULL AUTO_INCREMENT,
    phone_type VARCHAR(20),
    number VARCHAR(20) NOT NULL,
    PRIMARY KEY (id)
);

-- Customer Phones Table
CREATE TABLE Phones_customers (
    phone_id INT NOT NULL,
    customer_id INT NOT NULL,
    PRIMARY KEY (phone_id, customer_id),
    FOREIGN KEY (phone_id) REFERENCES Phones(id),
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
);

-- Store Phones Table
CREATE TABLE Phones_store (
    phone_id INT NOT NULL,
    store_id INT NOT NULL,
    PRIMARY KEY (phone_id, store_id),
    FOREIGN KEY (phone_id) REFERENCES Phones(id),
    FOREIGN KEY (store_id) REFERENCES Stores(id)
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
