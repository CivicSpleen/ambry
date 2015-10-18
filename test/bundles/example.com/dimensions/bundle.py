# -*- coding: utf-8 -*-
from ambry.bundle import Bundle
from ambry.etl import DatafileSourcePipe



class Bundle(Bundle):

    @staticmethod
    def doubleit1(v):
        return int(v) * 2

    @staticmethod
    def doubleit2(pipe, row, v):
        return int(v) * 2

    def doubleit3(self, row, v):
        return int(v) * 2

    def return1(self,pipe, row):
        return 1