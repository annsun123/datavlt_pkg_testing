# -*- coding: utf-8 -*-
"""
Created on Sun Dec  1 15:40:00 2019

@author: anyan.sun
"""

import logging
import datetime


class logging_func:
    def __init__(self,logger_name,filepath):
        self.logger_name=logger_name
        self.filepath=filepath
        
    def myLogger(self):

        logger=logging.getLogger(self.logger_name)
        logger.setLevel(logging.DEBUG)

        now = datetime.datetime.now()
        handler=logging.FileHandler(self.filepath + 'log/'+ self.logger_name +format(datetime.datetime.now(),"%m_%d_%Y")+'.log')

        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
