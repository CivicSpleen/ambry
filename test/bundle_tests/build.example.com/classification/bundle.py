# -*- coding: utf-8 -*-
import ambry.bundle



class Bundle(ambry.bundle.Bundle):

    pass

    def xform1(self, v):
        return v*2

    def xform2(self, v, row):
        from ambry.valuetype import ValueType
        if bool(v):
            return v*3
        elif isinstance(v, ValueType):
            print '!!!', v, v.failed_value

    def xform3(self, v):
        return v*3

    def xform4(self, v):
        return v*4

    def intl(self, v):
        return v   
        
    def excpt(self, v):
        return 1
    
                     
                     