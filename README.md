# Media Distributors Data Warehouse Project

## Project Overview

This project implements a cloud-based data warehouse solution for a media distributing company, integrating data from two separate operational databases (film rentals and music sales) into a unified analytics platform. The solution enables comprehensive business intelligence reporting to support the company's acquisition readiness.

## Architecture

### Source Systems
- **Film Database** (SQLite): Contains movie rental transactions, customer data, and payment information
- **Music Database** (SQLite): Contains music sales transactions, customer data, and invoice information

### Target System
- **Cloud MySQL Database** (Aiven): Hosts the integrated data warehouse with star schema design

## Implementation Components

### 1. Database Schema Creation (`createStarSchema.PractII.*.R`)
Creates the star schema structure in MySQL with:
- **Dimension Tables**: `dim_date`, `dim_customer`, `dim_location`, `dim_product`
- **Fact Table**: `fact_sales`
- **Summary Table**: `sales_summary` (for performance optimization)

### 2. ETL Process (`loadAnalyticsDB.PractII.*.R`)
Performs Extract, Transform, and Load operations:
- **Extract**: Pulls data from both SQLite databases
- **Transform**: 
  - Standardizes country names (USA → United States)
  - Offsets IDs to prevent conflicts
  - Calculates date dimensions
  - Maps transactions to dimension keys
- **Load**: Uses batch processing for efficient data insertion

### 3. Business Analytics Report (`BusinessAnalysis.PractII.*.Rmd`)
Generates comprehensive HTML report including:
- Revenue analysis by country and time period
- Customer distribution metrics
- Film vs Music revenue comparisons
- Units sold analysis with quarterly breakdowns
- Dynamic visualizations using ggplot2

## Key Features

### Data Integration
- **Unified Customer View**: Merges customers from both systems with unique identifiers
- **Standardized Schema**: Applies data validation techniques and standardization to keep the data normalized
- **Product Consolidation**: Combines films and music tracks with type differentiation

### Performance Optimizations
- **Batch Processing**: Inserts data in batches (100-1000 records) for 50-100x performance improvement
- **Strategic Indexing**: Creates indexes after bulk loading for optimal query performance
- **Pre-aggregated Summaries**: Maintains summary tables for fast analytical queries
- **Date Range Detection**: Dynamically determines data range from source systems

### Scalability Considerations
- **ID Offset Strategy**: Music IDs offset by 10,000 to prevent conflicts
- **Flexible Date Handling**: Automatically adapts to any date range in source data
- **Efficient Memory Usage**: Processes data in chunks to handle large datasets

## Star Schema Design Justification

### Why Star Schema was used?

The star schema was chosen for several compelling reasons:

#### 1. **Query Performance**
- **Simplified Joins**: Star schema requires fewer joins than normalized schemas
- **Predictable Query Paths**: All queries follow the pattern: Fact → Dimension
- **Optimized for Aggregations**: Perfect for SUM, COUNT, AVG operations common in analytics

#### 2. **Business User Friendly**
- **Intuitive Structure**: Mirrors how business users think about data
- **Self-Documenting**: Dimension names clearly indicate their content
- **Reduced Complexity**: Easier for non-technical users to understand

#### 3. **Flexibility for Analytics**
The schema supports all required analytical use cases:
- Revenue analysis by time period (year/quarter/month)
- Geographic analysis by country/city/state
- Product type comparisons (film vs music)
- Customer segmentation and counting

### Dimension Design Decisions

#### **dim_date (Date Dimension)**
```sql
date_key (YYYYMMDD format), date, year, quarter, month, month_name, day, week
```
**Justification**: 
- Pre-calculated time attributes eliminate date calculations in queries
- Supports easy filtering and grouping by any time period
- Integer key (YYYYMMDD) allows fast date range queries

#### **dim_customer (Customer Dimension)**
```sql
customer_key, customer_id, first_name, last_name, email, country, city, state, customer_type
```
**Justification**:
- Denormalized geographic data for faster queries
- customer_type field distinguishes film vs music customers
- Maintains original customer_id for traceability

#### **dim_location (Location Dimension)**
```sql
location_key, country, city, state, postal_code
```
**Justification**:
- Separate location dimension enables geographic drill-down
- Supports future expansion (e.g., adding regions, territories)
- Reduces redundancy in fact table

#### **dim_product (Product Dimension)**
```sql
product_key, product_id, product_name, product_type, category, genre
```
**Justification**:
- Unified view of films and music tracks
- product_type enables easy filtering by business line
- Category (films) and genre (music) support detailed analysis

### Fact Table Design

#### **fact_sales (Sales Fact Table)**
```sql
fact_key, date_key, customer_key, location_key, product_key,
units_sold, revenue, year, quarter, month, product_type, country
```
**Justification**:
- **Grain**: One row per transaction (finest level of detail)
- **Redundant Columns**: year, quarter, month, product_type, country included for:
  - Partition potential for very large datasets
  - Faster queries without joining dimensions
  - Support for aggregate table creation
- **Measures**: units_sold and revenue support all required calculations

### Alternative Approaches Considered

#### 1. **Snowflake Schema**
- **Rejected because**: Additional normalization would slow queries
- Star schema's denormalization is acceptable for analytics workloads

#### 2. **One Big Table (OBT)**
- **Rejected because**: 
  - Would duplicate customer and product data excessively
  - Harder to maintain data quality
  - Less flexible for adding new dimensions

#### 3. **Separate Fact Tables**
- **Rejected because**:
  - Would require UNION operations for combined analysis
  - More complex for business users
  - Defeats purpose of integration

## Performance Considerations

### Indexing Strategy
- Primary keys on all dimension tables
- Composite indexes on fact table for common query patterns
- Indexes on summary table for rapid aggregations

### Data Volume Projections
- Film rentals: ~16K transactions
- Music sales: ~2K invoices with multiple line items
- Total fact records: ~20-30K (easily handled by MySQL)

### Query Optimization
- Pre-aggregated summary table for common queries
- Date dimension eliminates expensive date calculations
- Denormalized geographic data avoids multiple joins

## Maintenance and Extensions

### Future Enhancements
1. **Slowly Changing Dimensions**: Track customer address changes
2. **Additional Facts**: Returns, cancellations, customer lifetime value
3. **New Dimensions**: Employee, promotion, seasonality
4. **Real-time Updates**: Incremental ETL for daily refreshes

### Monitoring Recommendations
- Track ETL execution times
- Monitor query performance
- Check data quality metrics
- Validate row counts between source and target

## Conclusion

This star schema design provides an optimal balance of:
- **Performance**: Fast query execution for analytics
- **Usability**: Intuitive structure for business users
- **Flexibility**: Supports current and future analytical needs
- **Maintainability**: Clear design simplifies troubleshooting

The implementation successfully integrates two disparate systems into a unified analytical platform, enabling Media Distributors, Inc. to present comprehensive business metrics to potential acquirers.
