# Program Name: loadAnalyticsDB.PractII.SavithriAnbarasuH.R
# Author: Hariharasudan Savithri Anbarasu
# Semester: Full Summer 2025

if (!require("DBI")) install.packages("DBI")
if (!require("RMySQL")) install.packages("RMySQL")
if (!require("RSQLite")) install.packages("RSQLite")

library(DBI)
library(RMySQL)
library(RSQLite)

# Set sqldf to use SQLite to avoid conflicts
options(sqldf.driver = 'SQLite')

# Database connection function for the Aiven DB
connectMySQL <- function(){
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
    cat("Connected to MySQL DB\n")
  }, error = function(e) {
    cat("Error:", e$message, "\n")
    stop("Program ends since DB connection failed.")
  })
  
  return(con)
}

# Connect to SQLite DBs (Music and Film DBs)
connectSQLite <- function(db_path) {
  tryCatch({
    con <- dbConnect(SQLite(), db_path)
    cat("Connected to SQLite DB:", db_path, "\n")
    return(con)
  }, error = function(e) {
    cat("Error connecting to SQLite:", e$message, "\n")
    stop()
  })
}

# Function to populate date dimension
populateDateDimension <- function(mysql_con, film_con, music_con) {
  cat("\nPopulating date dimension...\n")
  
  # Get date range from both databases
  film_dates_rental <- dbGetQuery(film_con, "
    SELECT MIN(DATE(rental_date)) as min_date, MAX(DATE(rental_date)) as max_date 
    FROM rental 
    WHERE rental_date IS NOT NULL
  ")
  
  film_dates_payment <- dbGetQuery(film_con, "
    SELECT MIN(DATE(payment_date)) as min_date, MAX(DATE(payment_date)) as max_date 
    FROM payment 
    WHERE payment_date IS NOT NULL
  ")
  
  # Get the overall min/max for film database
  film_min_date <- min(film_dates_rental$min_date, film_dates_payment$min_date)
  film_max_date <- max(film_dates_rental$max_date, film_dates_payment$max_date)
  
  music_dates <- dbGetQuery(music_con, "
    SELECT MIN(DATE(InvoiceDate)) as min_date, MAX(DATE(InvoiceDate)) as max_date 
    FROM invoices 
    WHERE InvoiceDate IS NOT NULL
  ")
  
  # Determine overall date range
  start_date <- as.Date(min(film_min_date, music_dates$min_date))
  end_date <- as.Date(max(film_max_date, music_dates$max_date))
  
  cat("Date range found: from", as.character(start_date), "to", as.character(end_date), "\n")
  
  # Add some buffer days
  start_date <- start_date - 30
  end_date <- end_date + 30
  
  dates <- seq(start_date, end_date, by = "day")
  
  # Create date dimension data
  date_dim <- data.frame(
    date_key = as.integer(format(dates, "%Y%m%d")),
    date = dates,
    year = year(dates),
    quarter = quarter(dates),
    month = month(dates),
    month_name = format(dates, "%B"),
    day = day(dates),
    week = week(dates)
  )
  
  # Insert into Date Dimension table in batches
  batch_size <- 1000
  n_batches <- ceiling(nrow(date_dim) / batch_size)
  
  for (i in 1:n_batches) {
    start_idx <- (i - 1) * batch_size + 1
    end_idx <- min(i * batch_size, nrow(date_dim))
    batch <- date_dim[start_idx:end_idx, ]
    
    # Create Values Clause
    values <- paste0("(", 
                     batch$date_key, ", '", 
                     batch$date, "', ", 
                     batch$year, ", ", 
                     batch$quarter, ", ", 
                     batch$month, ", '", 
                     batch$month_name, "', ", 
                     batch$day, ", ", 
                     batch$week, ")", 
                     collapse = ", ")
    
    insert_sql <- paste0("INSERT IGNORE INTO dim_date (date_key, date, year, quarter, month, month_name, day, week) VALUES ", values)
    
    tryCatch({
      dbExecute(mysql_con, insert_sql)
    }, error = function(e) {
      cat("Error inserting date batch", i, ":", e$message, "\n")
    })
  }
  
  cat("Date dimension loaded with", nrow(date_dim), "records\n")
}

# Function to load location dimension
loadLocationDimension <- function(mysql_con, film_con, music_con) {
  cat("\nLoading location dimension...\n")
  
  # Extract unique locations from film database
  film_locations_sql <- "
    SELECT DISTINCT 
      co.country,
      ci.city,
      NULL as state,
      NULL as postal_code
    FROM customer c
    JOIN address a ON c.address_id = a.address_id
    JOIN city ci ON a.city_id = ci.city_id
    JOIN country co ON ci.country_id = co.country_id
  "
  
  # Extract unique locations from music database
  music_locations_sql <- "
    SELECT DISTINCT
      CASE 
      WHEN Country = 'USA' THEN 'United States'
      ELSE Country 
      END as country,
      City as city,
      State as state,
      PostalCode as postal_code
    FROM customers
    WHERE Country IS NOT NULL
  "
  
  film_locations <- dbGetQuery(film_con, film_locations_sql)
  music_locations <- dbGetQuery(music_con, music_locations_sql)
  
  # Combine and deduplicate locations
  all_locations <- rbind(film_locations, music_locations)
  all_locations <- unique(all_locations)
  
  # Insert locations in batches
  batch_size <- 100
  n_batches <- ceiling(nrow(all_locations) / batch_size)
  
  for (i in 1:n_batches) {
    start_idx <- (i - 1) * batch_size + 1
    end_idx <- min(i * batch_size, nrow(all_locations))
    batch <- all_locations[start_idx:end_idx, ]
    
    # Build batch insert values
    values_list <- character(nrow(batch))
    for (j in 1:nrow(batch)) {
      loc <- batch[j, ]
      values_list[j] <- sprintf(
        "('%s', %s, %s, %s)",
        gsub("'", "''", loc$country),
        ifelse(is.na(loc$city), "NULL", paste0("'", gsub("'", "''", loc$city), "'")),
        ifelse(is.na(loc$state), "NULL", paste0("'", gsub("'", "''", loc$state), "'")),
        ifelse(is.na(loc$postal_code), "NULL", paste0("'", gsub("'", "''", loc$postal_code), "'"))
      )
    }
    
    insert_sql <- paste0(
      "INSERT IGNORE INTO dim_location (country, city, state, postal_code) VALUES ",
      paste(values_list, collapse = ", ")
    )
    
    tryCatch({
      dbExecute(mysql_con, insert_sql)
    }, error = function(e) {
      cat("Error in location batch", i, ":", e$message, "\n")
    })
  }
  
  cat("Location dimension loaded with", nrow(all_locations), "records\n")
}

# Function to load customer dimension
loadCustomerDimension <- function(mysql_con, film_con, music_con) {
  cat("\nLoading customer dimension...\n")
  
  # Extract film customers
  film_customers_sql <- "
    SELECT 
      c.customer_id,
      c.first_name,
      c.last_name,
      c.email,
      co.country,
      ci.city,
      NULL as state,
      'film' as customer_type
    FROM customer c
    JOIN address a ON c.address_id = a.address_id
    JOIN city ci ON a.city_id = ci.city_id
    JOIN country co ON ci.country_id = co.country_id
  "
  
  # Extract music customers
  music_customers_sql <- "
    SELECT 
      CustomerId as customer_id,
      FirstName as first_name,
      LastName as last_name,
      Email as email,
      CASE 
      WHEN Country = 'USA' THEN 'United States'
      ELSE Country 
      END as country,
      City as city,
      State as state,
      'music' as customer_type
    FROM customers
  "
  
  film_customers <- dbGetQuery(film_con, film_customers_sql)
  music_customers <- dbGetQuery(music_con, music_customers_sql)
  
  # Offset music customer IDs to avoid conflicts (since customer ids are overlapping)
  music_customers$customer_id <- music_customers$customer_id + 10000
  
  # Insert customers in batches
  all_customers <- rbind(film_customers, music_customers)
  
  batch_size <- 500
  n_batches <- ceiling(nrow(all_customers) / batch_size)
  
  for (i in 1:n_batches) {
    start_idx <- (i - 1) * batch_size + 1
    end_idx <- min(i * batch_size, nrow(all_customers))
    batch <- all_customers[start_idx:end_idx, ]
    
    # Build batch insert values
    values_list <- character(nrow(batch))
    for (j in 1:nrow(batch)) {
      cust <- batch[j, ]
      values_list[j] <- sprintf(
        "(%d, '%s', '%s', %s, '%s', %s, %s, '%s')",
        cust$customer_id,
        gsub("'", "''", cust$first_name),
        gsub("'", "''", cust$last_name),
        ifelse(is.na(cust$email), "NULL", paste0("'", gsub("'", "''", cust$email), "'")),
        gsub("'", "''", cust$country),
        ifelse(is.na(cust$city), "NULL", paste0("'", gsub("'", "''", cust$city), "'")),
        ifelse(is.na(cust$state), "NULL", paste0("'", gsub("'", "''", cust$state), "'")),
        cust$customer_type
      )
    }
    
    insert_sql <- paste0(
      "INSERT INTO dim_customer (customer_id, first_name, last_name, email, country, city, state, customer_type) VALUES ",
      paste(values_list, collapse = ", ")
    )
    
    tryCatch({
      dbExecute(mysql_con, insert_sql)
      if (i %% 10 == 0) {
        cat("Processed customer batch", i, "of", n_batches, "\n")
      }
    }, error = function(e) {
      cat("Error in customer batch", i, ":", e$message, "\n")
    })
  }
  
  cat("Customer dimension loaded with", nrow(all_customers), "records\n")
}

# Function to load product dimension
loadProductDimension <- function(mysql_con, film_con, music_con) {
  cat("\nLoading product dimension...\n")
  
  # Extract films
  films_sql <- "
    SELECT 
      f.film_id as product_id,
      f.title as product_name,
      'film' as product_type,
      c.name as category,
      NULL as genre
    FROM film f
    LEFT JOIN film_category fc ON f.film_id = fc.film_id
    LEFT JOIN category c ON fc.category_id = c.category_id
  "
  
  # Extract music tracks
  music_sql <- "
    SELECT 
      t.TrackId as product_id,
      t.Name as product_name,
      'music' as product_type,
      NULL as category,
      g.Name as genre
    FROM tracks t
    LEFT JOIN genres g ON t.GenreId = g.GenreId
  "
  
  films <- dbGetQuery(film_con, films_sql)
  music <- dbGetQuery(music_con, music_sql)
  
  # Offset music product IDs
  music$product_id <- music$product_id + 10000
  
  # Combine products
  all_products <- rbind(films, music)
  
  # Insert products in batches
  batch_size <- 500
  n_batches <- ceiling(nrow(all_products) / batch_size)
  
  for (i in 1:n_batches) {
    start_idx <- (i - 1) * batch_size + 1
    end_idx <- min(i * batch_size, nrow(all_products))
    batch <- all_products[start_idx:end_idx, ]
    
    # Build batch insert values
    values_list <- character(nrow(batch))
    for (j in 1:nrow(batch)) {
      prod <- batch[j, ]
      values_list[j] <- sprintf(
        "(%d, '%s', '%s', %s, %s)",
        prod$product_id,
        gsub("'", "''", substr(prod$product_name, 1, 255)),
        prod$product_type,
        ifelse(is.na(prod$category), "NULL", paste0("'", gsub("'", "''", prod$category), "'")),
        ifelse(is.na(prod$genre), "NULL", paste0("'", gsub("'", "''", prod$genre), "'"))
      )
    }
    
    insert_sql <- paste0(
      "INSERT INTO dim_product (product_id, product_name, product_type, category, genre) VALUES ",
      paste(values_list, collapse = ", ")
    )
    
    tryCatch({
      dbExecute(mysql_con, insert_sql)
      if (i %% 10 == 0) {
        cat("Processed product batch", i, "of", n_batches, "\n")
      }
    }, error = function(e) {
      cat("Error in product batch", i, ":", e$message, "\n")
    })
  }
  
  cat("Product dimension populated with", nrow(all_products), "records\n")
}

# Function to extract and load fact data
loadFactData <- function(mysql_con, film_con, music_con) {
  cat("\nLoading fact data...\n")
  
  # Get the dimension keys
  location_keys <- dbGetQuery(mysql_con, "SELECT location_key, country, city, state FROM dim_location")
  customer_keys <- dbGetQuery(mysql_con, "SELECT customer_key, customer_id FROM dim_customer")
  product_keys <- dbGetQuery(mysql_con, "SELECT product_key, product_id, product_type FROM dim_product")
  
  # Extract film rentals with revenue
  film_facts_sql <- "
    SELECT 
      DATE(p.payment_date) as transaction_date,
      c.customer_id,
      co.country,
      ci.city,
      NULL as state,
     COALESCE(i.film_id, -1) as product_id,
      1 as units_sold,
      p.amount as revenue
    FROM payment p
    JOIN customer c ON p.customer_id = c.customer_id
    JOIN address a ON c.address_id = a.address_id
    JOIN city ci ON a.city_id = ci.city_id
    JOIN country co ON ci.country_id = co.country_id
    LEFT JOIN rental r ON p.rental_id = r.rental_id
    LEFT JOIN inventory i ON r.inventory_id = i.inventory_id
    LEFT JOIN film f ON i.film_id = f.film_id
    WHERE p.payment_date IS NOT NULL
      AND p.amount > 0
  "
  
  # Extract music sales
  music_facts_sql <- "
    SELECT 
      DATE(i.InvoiceDate) as transaction_date,
      c.CustomerId as customer_id,
      CASE 
        WHEN c.Country = 'USA' THEN 'United States'
        ELSE c.Country 
      END as country,
      c.City as city,
      c.State as state,
      ii.TrackId as product_id,
      ii.Quantity as units_sold,
      (ii.UnitPrice * ii.Quantity) as revenue
    FROM invoices i
    JOIN invoice_items ii ON i.InvoiceId = ii.InvoiceId
    JOIN customers c ON i.CustomerId = c.CustomerId
    WHERE i.InvoiceDate IS NOT NULL
  "
  
  cat("Extracting film transactions...\n")
  film_facts <- dbGetQuery(film_con, film_facts_sql)
  
  cat("Extracting music transactions...\n")
  music_facts <- dbGetQuery(music_con, music_facts_sql)
  
  # Adjust customer and product IDs for music
  music_facts$customer_id <- music_facts$customer_id + 10000
  music_facts$product_id <- music_facts$product_id + 10000
  
  # Process and insert facts in batches
  process_facts <- function(facts, product_type) {
    batch_size <- 500  # Increased batch size for facts
    n_batches <- ceiling(nrow(facts) / batch_size)
    
    for (i in 1:n_batches) {
      if (i %% 10 == 0) {
        cat("Processing", product_type, "batch", i, "of", n_batches, "\n")
      }
      
      start_idx <- (i - 1) * batch_size + 1
      end_idx <- min(i * batch_size, nrow(facts))
      batch <- facts[start_idx:end_idx, ]
      
      # Build batch insert values
      values_list <- character(nrow(batch))
      
      for (j in 1:nrow(batch)) {
        fact <- batch[j, ]
        
        # Get dimension keys
        date_key <- as.integer(format(as.Date(fact$transaction_date), "%Y%m%d"))
        
        # Find location key
        loc_match <- which(location_keys$country == fact$country & 
                             (is.na(location_keys$city) | location_keys$city == fact$city) &
                             (is.na(location_keys$state) | location_keys$state == fact$state))
        location_key <- ifelse(length(loc_match) > 0, location_keys$location_key[loc_match[1]], 1)
        
        # Find customer key
        cust_match <- which(customer_keys$customer_id == fact$customer_id)
        customer_key <- ifelse(length(cust_match) > 0, customer_keys$customer_key[cust_match[1]], "NULL")
        
        # Find product key
        prod_match <- which(product_keys$product_id == fact$product_id)
        product_key <- ifelse(length(prod_match) > 0, product_keys$product_key[prod_match[1]], 1)
        
        # Extract date components
        trans_date <- as.Date(fact$transaction_date)
        year_val <- year(trans_date)
        quarter_val <- quarter(trans_date)
        month_val <- month(trans_date)
        
        values_list[j] <- sprintf(
          "(%d, %s, %d, %d, %d, %.2f, %d, %d, %d, '%s', '%s')",
          date_key,
          customer_key,
          location_key,
          product_key,
          fact$units_sold,
          fact$revenue,
          year_val,
          quarter_val,
          month_val,
          product_type,
          gsub("'", "''", fact$country)
        )
      }
      
      # Execute batch insert
      insert_sql <- paste0(
        "INSERT INTO fact_sales (date_key, customer_key, location_key, product_key, ",
        "units_sold, revenue, year, quarter, month, product_type, country) VALUES ",
        paste(values_list, collapse = ", ")
      )
      
      tryCatch({
        dbExecute(mysql_con, insert_sql)
      }, error = function(e) {
        cat("Error in", product_type, "fact batch", i, ":", e$message, "\n")
      })
    }
    
    cat("Completed loading", nrow(facts), product_type, "facts\n")
  }
  
  # Process film and music facts
  cat("Loading film facts...\n")
  process_facts(film_facts, "film")
  
  cat("Loading music facts...\n")
  process_facts(music_facts, "music")
  
  cat("Fact data loaded\n")
}

# Main execution
main <- function() {
  cat("=== ETL Process for Media Distributors Analytics ===\n\n")
  
  # Connect to DBs
  mysql_con <- connectMySQL()
  film_con <- connectSQLite("film-sales.db")
  music_con <- connectSQLite("music-sales.db")
  
  # Populate dimensions
  populateDateDimension(mysql_con, film_con, music_con)
  loadLocationDimension(mysql_con, film_con, music_con)
  loadCustomerDimension(mysql_con, film_con, music_con)
  loadProductDimension(mysql_con, film_con, music_con)
  
  # Load fact data
  loadFactData(mysql_con, film_con, music_con)
  
  # Close connections
  dbDisconnect(mysql_con)
  dbDisconnect(film_con)
  dbDisconnect(music_con)
  
  cat("\nDB connections closed\n")
}

# Run the main function
main()