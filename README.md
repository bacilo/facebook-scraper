# facebook-scraper
A set of python modules to help scrape data from Facebook using the Graph API and store it in different formats

## Requirements
This project has been developed on OSX using Python's Anaconda 4.3.1 (Python 3.6) which comes pre-packaged with all the necessary libraries for running this project. I do not provide support for Windows or Linux, nor to users who are not using the Anaconda distribution.

Anaconda: https://www.continuum.io/downloads

## Usage
Please refer to the Jupyter notebook for step-by-step illustrations of how it works:
[Scraping Basics](ScrapingBasics.ipynb)

## Sample Script
One quick way to get started would be to create a python script that scrapes a page given the proper credentials, for instance:
``` python
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import fb_scraper.prodcons

APP_ID = ''
APP_ID_SECRET = ''
ACCESS_TOKEN = ''

def main():
    mgr = fb_scraper.prodcons.Manager(
        access_token=ACCESS_TOKEN,
        api_key=APP_ID,
        api_secret=APP_ID_SECRET
        )
    mgr.start()

    mgr.scrape_group('group_id')  # Add group_id
    mgr.scrape_post('post_id')  # Add full form post_id (i.e. groupid_postid)

if __name__ == "__main__":
    main()
```
And run the script to fully scrape the Public Page/Group PAGE_ID

## Limitations
Apart from the non-implemented aspects, this scraper is subjected to the limitations/possibilities provided by the Graph API (i.e. cannot scrape closed groups unless it is the admin doing it or inability to scrape a profile page). Please refer to the Graph API Documentation for an understanding of those limitations