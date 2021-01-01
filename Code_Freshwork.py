#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
import time
import threading
import uuid
import os
import sys

def get_file_time():
    import uuid
    uniq_append_string = uuid.uuid4().hex
    return "LOCAL_STORAGE_{}".format(uniq_append_string)

def get_inst(file_name=None):
    if file_name is None:
        file_name = get_file_time()
    try:
        os.mkdir('C:/tmp')
    except:
        full_file_name = f"{'C:/tmp'}/{file_name+'.txt'}"
    return full_file_name


class DS:
    
    def __init__(self, file_desc=None):
        self.__lock = threading.Lock()
        if(file_desc != None and os.path.isfile(file_desc)):
            self.file_desc = file_desc
        else:
            self.file_desc = get_inst()
        self.f = open(self.file_desc, 'w+')
        self.c = dict(self.f.read())
        
    def create(self, key, value, timeout=0):
        self.__lock.acquire()
        try:
            self.key = key
            self.value = value
            self.timeout = timeout
            if self.key in self.c:
                print("error: this key already exists")
            else:
                if(self.key.isalpha()):
                    if (len(self.c) < (1024*1024*1024) and sys.getsizeof(self.value) <= (16*1024)):
                        if (self.timeout == 0):
                            self.l = [self.value, self.timeout]
                        else:
                            self.l = [self.value, time.time()+self.timeout]
                        
                        if (len(self.key) <= 32):  
                            self.c[self.key] = self.l
                        else:
                            print("error: Key Size must be between 32 character")
                    else:
                        print("error: Memory limit exceeded!!")
                else:
                    print(
                        "error: Invalid key_name!! key_name must contain only alphabets and no special characters or numbers")
        finally:
            self.__lock.release()

    def read(self, key):
        self.__lock.acquire()
        try:
            self.json = {}
            self.key = key
            if (self.key not in self.c):
                print("error: given key does not exist in database. Please enter a valid key")
            else:
                self.b = self.c[key]
                if (self.b[1] != 0):
                    if (time.time() < self.b[1]):
                        if(type(self.b[0]) != dict()):
                            res = dict(self.b[0])

                        return res
                    else:
                        print("error: time-to-live of", self.key,"has expired")
                else:
                    if(type(self.b[0]) != dict()):
                        res = dict(self.b[0])

                    return res
        finally:
            self.__lock.release()

    def delete(self, key):
        self.key = key
        if (self.key not in self.c):
            raise "error: given key does not exist in database. Please enter a valid key"
        else:
            self.b = self.c[key]
            if (self.b[1] != 0):
                if (time.time() < self.b[1]):
                    del self.c[key]
                    print("key is successfully deleted")
                else:
                    print("error: time-to-live of", self.key,"has expired") 
            else:
                del self.c[key]
                print("key is successfully deleted")

    def save(self):
        x = json.dumps(self.c, indent=3)
        self.f.write(x)
        self.f.close()


# In[ ]:




