# -*- coding: utf-8 -*-
"""
Created on Sun Oct 18 19:29:54 2020

@author: 605453
"""

import Revise

prac = Revise.Scraper("06-05-2019", "06-15-2019",
               "C:/Users/605453/Downloads/",
               "gdelt")

prac.scrape(GKG=True, Mentions=True, Events=True)