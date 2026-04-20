# Japan Tech Job Tracker

A personal project that aggregates publicly available job listings from selected global tech companies and tracks roles relevant to Japan.  
The goal of the project is to monitor new openings from companies with strong product and engineering cultures and maintain a lightweight dashboard to review opportunities.

It is not affiliated with any companies or job platforms.  
The project only accesses publicly available endpoints and does not bypass authentication or paywalls.

---

## Motivation

Many global tech companies hire in Japan, but their job listings are scattered across different career platforms and Applicant Tracking Systems (ATS).  
This project was built to solve a simple problem:

* Monitor job openings across multiple companies in one place  
* Detect newly posted roles quickly  
* Track when roles are removed or filled  
* Focus specifically on opportunities relevant to Japan  

The project acts as a small **job intelligence system**, helping surface opportunities that might otherwise be missed.

---

## Live Demo
Streamlit dashboard:  
https://tokyojobs.streamlit.app/

---

## Overview

Many companies use different Applicant Tracking Systems or custom-built career pages to publish job listings.  
This project collects job postings from multiple systems and consolidates them into a single database and dashboard.

Currently supported sources include:

* Greenhouse  
* Lever  
* Workday  
* SmartRecruiters  
* Ashby  
* Eightfold  
* BambooHR  
* Custom APIs (e.g. GraphQL endpoints)  
* Custom HTML-based career pages  

The scraper collects job metadata such as:

* Company  
* Job title  
* Location  
* Job URL  
* First seen timestamp  
* Last seen timestamp  
* Active / removed status  

---

## Architecture

Scraping scripts run periodically and store results in a database.  
A dashboard then queries the database to visualize the results.


Company Career Sites
│
▼
Python Scrapers
(API + HTML + GraphQL)
│
▼
Supabase
(Postgres Database)
│
▼
Streamlit Cloud App


---

## Tech Stack

* Python  
* Supabase (Postgres database)  
* Streamlit (hosted on Streamlit Cloud)  
* GitHub / GitLab for source control  
* Scheduled jobs (GitHub Actions / GitLab CI) for automated scraping  

---

## Features

* Aggregates job postings across multiple ATS platforms and custom career pages  
* Tracks newly posted roles  
* Tracks when jobs are removed  
* Filters for roles relevant to Japan  
* Lightweight dashboard for reviewing opportunities  
* Near real-time updates via scheduled scraping  

---

## Running the Scraper

Install dependencies:


pip install -r requirements.txt


Run the scraper:


python scraper.py


---

## Dashboard

The dashboard is deployed on Streamlit Cloud and connected directly to Supabase.  
Environment variables (e.g. database credentials) are managed securely via Streamlit app settings.

To run locally:


streamlit run app.py


---

## Disclaimer

This project collects publicly available job listings from company career pages for personal tracking and research purposes.

All job data belongs to the respective companies.  
This project is not affiliated with or endorsed by any of the companies referenced.

---

## License

MIT License