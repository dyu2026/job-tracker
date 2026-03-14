# Japan Tech Job Tracker

A personal project that aggregates publicly available job listings from selected global tech companies and tracks roles relevant to Japan.
The goal of the project is to monitor new openings from companies with strong product and engineering cultures and maintain a lightweight dashboard to review opportunities.

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

## Overview

Many companies use different Applicant Tracking Systems to publish job listings.
This project collects job postings from multiple systems and consolidates them into a single database and dashboard.

Currently supported ATS platforms include:

* Greenhouse
* Lever
* Workday
* SmartRecruiters
* Ashby
* Eightfold

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

```
Company Career Sites
        │
        ▼
   Python Scrapers
        │
        ▼
     Supabase
(Postgres Database)
        │
        ▼
  Streamlit Dashboard
```

---

## Tech Stack

* Python
* Supabase (Postgres database)
* Streamlit dashboard
* GitHub / GitLab for source control
* Scheduled jobs for automated scraping

---

## Features

* Aggregates job postings across multiple ATS platforms
* Tracks newly posted roles
* Tracks when jobs are removed
* Filters for roles relevant to Japan
* Simple dashboard for reviewing opportunities

---

## Running the Scraper

Install dependencies:

```
pip install -r requirements.txt
```

Run the scraper:

```
python scraper.py
```

---

## Dashboard

To launch the dashboard:

```
streamlit run app.py
```

---

## Disclaimer

This project collects publicly available job listings from company career pages for personal tracking and research purposes.

All job data belongs to the respective companies.
This project is not affiliated with or endorsed by any of the companies referenced.

---

## License

MIT License