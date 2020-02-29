# -*- coding: utf-8 -*-
"""
Created on Thu Feb  6 08:19:05 2020

@author: anyan
"""

import logging
import datetime
class logging_func:
    def __init__(self,logger_name,filepath):
        self.logger_name=logger_name
        
        
    def myLogger(self):

        logger=logging.getLogger(self.logger_name)
        logger.setLevel(logging.DEBUG)

        now = datetime.datetime.now()
        handler=logging.FileHandler('./json_function/log/'+ self.logger_name + '_' + format(datetime.datetime.now(),"%m_%d_%Y")+'.log')

        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
