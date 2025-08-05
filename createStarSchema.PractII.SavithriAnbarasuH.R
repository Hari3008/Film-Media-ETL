# Program Name: createStarSchema.PractII.SavithriAnbarasuH.R
# Author: Hariharasudan Savithri Anbarasu
# Semester: Full Summer 2025

# Load required libraries
if (!require("DBI")) install.packages("DBI")
if (!require("RMySQL")) install.packages("RMySQL")
if (!require("RSQLite")) install.packages("RSQLite")

library(DBI)
library(RMySQL)
library(RSQLite)

connectDB <- function(){
  db_user <- "avnadmin"
  db_password <- "AVNS_s5fw-5CC0zMcdySooAU"
  db_host <- "mysql-practicum-1-practicum-1-hariharasudan.f.aivencloud.com"
  db_port <- 28796
  db_name <- "defaultdb"
  
  # DB Connection
  tryCatch({
    con <- dbConnect(
      MySQL(),
      user = db_user,
      password = db_password,
      host = db_host,
      port = db_port,
      dbname = db_name
    )
    cat("Connected to DB\n")
  }, error = function(e) {
    cat("Error:", e$message, "\n")
    stop("DB connection failed")
  })
  
  return(con)
}

# Function to drop existing tables
dropExistingTables <- function(con) {
  tables <- c("fact_sales", "dim_date", "dim_customer", "dim_location", "dim_product")
  
  for (table in tables) {
    tryCatch({
      query <- paste0("DROP TABLE IF EXISTS ", table)
      dbExecute(con, query)
      cat("Dropped table:", table, "\n")
    }, error = function(e) {
      cat("Error dropping table", table, ":", e$message, "\n")
    })
  }
}

# Function to create dimension tables
createDimensionTables <- function(con) {
  
  # Create Date Dimension Table
  date_dim_sql <- "
  CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,
    date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    INDEX idx_date (date),
    INDEX idx_year_month (year, month)
  )"
  
  # Create Customer Dimension Table
  customer_dim_sql <- "
  CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key INT PRIMARY KEY AUTO_INCREMENT,
    customer_id INT NOT NULL,
    first_name VARCHAR(45),
    last_name VARCHAR(45),
    email VARCHAR(60),
    country VARCHAR(50),
    city VARCHAR(50),
    state VARCHAR(50),
    customer_type VARCHAR(20) NOT NULL,
    INDEX idx_customer_id (customer_id),
    INDEX idx_country (country)
  )"
  
  # Create Location Dimension Table
  location_dim_sql <- "
  CREATE TABLE IF NOT EXISTS dim_location (
    location_key INT PRIMARY KEY AUTO_INCREMENT,
    country VARCHAR(50) NOT NULL,
    city VARCHAR(50),
    state VARCHAR(50),
    postal_code VARCHAR(10),
    UNIQUE KEY unique_location (country, city, state),
    INDEX idx_country (country)
  )"
  
  # Create Product Dimension Table
  product_dim_sql <- "
  CREATE TABLE IF NOT EXISTS dim_product (
    product_key INT PRIMARY KEY AUTO_INCREMENT,
    product_id INT NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    product_type VARCHAR(20) NOT NULL,
    category VARCHAR(50),
    genre VARCHAR(50),
    INDEX idx_product_type (product_type),
    INDEX idx_product_id (product_id)
  )"
  
  # Execute create table statements
  tryCatch({
    dbExecute(con, date_dim_sql)
    cat("Created dim_date table\n")
    
    dbExecute(con, customer_dim_sql)
    cat("Created dim_customer table\n")
    
    dbExecute(con, location_dim_sql)
    cat("Created dim_location table\n")
    
    dbExecute(con, product_dim_sql)
    cat("Created dim_product table\n")
    
  }, error = function(e) {
    cat("Error creating dimension tables:", e$message, "\n")
    stop()
  })
}

# Function to create fact table
createFactTable <- function(con) {
  
  # Create Sales Fact Table with pre-aggregated measures
  fact_sales_sql <- "
  CREATE TABLE IF NOT EXISTS fact_sales (
    fact_key INT PRIMARY KEY AUTO_INCREMENT,
    date_key INT NOT NULL,
    customer_key INT,
    location_key INT NOT NULL,
    product_key INT NOT NULL,
    
    units_sold INT NOT NULL DEFAULT 0,
    revenue DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    
    revenue_ytd DECIMAL(12,2),
    revenue_qtd DECIMAL(12,2),
    revenue_mtd DECIMAL(12,2),
    units_ytd INT,
    units_qtd INT,
    units_mtd INT,
    
    product_type VARCHAR(20) NOT NULL,
    country VARCHAR(50) NOT NULL,
    
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (location_key) REFERENCES dim_location(location_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    
    INDEX idx_date (date_key),
    INDEX idx_year_month (year, month),
    INDEX idx_country_type (country, product_type),
    INDEX idx_quarter (quarter),
    INDEX idx_composite (year, quarter, month, country, product_type)
  )"
  
  tryCatch({
    dbExecute(con, fact_sales_sql)
    cat("Created fact_sales table\n")
  }, error = function(e) {
    cat("Error creating fact table:", e$message, "\n")
    stop()
  })
}

# Main execution
main <- function() {
  cat("Creating Star Schema for Media Distributors\n\n")
  
  # Connect to DB
  con <- connectDB()
  
  # Drop existing tables
  cat("\nDropping existing tables...\n")
  dropExistingTables(con)
  
  # Create dimension tables
  cat("\nCreating dimension tables...\n")
  createDimensionTables(con)
  
  # Create fact table
  cat("\nCreating fact table...\n")
  createFactTable(con)
  
  cat("\nSchema Created\n")
  
  # Disconnect DB
  dbDisconnect(con)
  cat("\nDisconnected from DB\n")
}

main()