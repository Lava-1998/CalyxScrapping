# CalyxScrapping
Assignment for Calyx Scrapping

This project is an assignment focused on web scraping data from a project registry and loading the extracted data into a database.

Overview
  The repository provides a web scraping solution to extract project data from Gold Standard Registry. After scraping, the data is processed and loaded into a database for further use.

Steps
  1) Import Required Packages: Ensure all necessary Python packages for scraping and database operations are imported.

  2) dbconnection(): Establish a connection to the database, setting up a strong connection string.

  3) scraper(): This function handles the web scraping process. It extracts all necessary data points from the project listings on the Gold Standard Registry.

  4) Convert Data to CSV: The extracted data is converted into a CSV file for easy handling and processing.

  5) Load CSV into Database: The CSV data is loaded into the database for future analysis or reporting.


Requirements
  Install required packages mentioned : pip install requests beautifulsoup4 pandas pyodbc
