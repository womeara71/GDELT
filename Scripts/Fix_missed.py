# -*- coding: utf-8 -*-
"""
Created on Tue Oct 20 19:14:07 2020

@author: 605453
"""

import datetime


def missedDates(beg_date, end_date, GKG=False, Mentions=False, Events=False):
    beg_year, beg_month, beg_day, beg_hour, beg_min = int(beg_date[:4]), int(beg_date[4:6]), int(beg_date[6:8]), int(beg_date[8:10]), int(beg_date[10:12])
    end_year, end_month, end_day, end_hour, end_min = int(end_date[:4]), int(end_date[4:6]), int(end_date[6:8]), int(end_date[8:10]), int(end_date[10:12])
    
    
    start = datetime.datetime(year = beg_year, month = beg_month, day = beg_day, hour = beg_hour, minute = beg_min)
    end = datetime.datetime(year = end_year, month = end_month, day = end_day, hour = end_hour, minute = end_min)    
    days_to_collect = end - start
    
    date_list = [end]
    date_list.extend([end - datetime.timedelta(minutes=15*x) for x in range(1, int((days_to_collect.total_seconds()/60)/15))])
    date_list.extend([start])
    date_list = list(map(lambda x: x.strftime("%Y%m%d%H%M") + '00', date_list))
    return date_list


prac = missedDates("20190613031500", "20190615034500")