# -*- coding: utf-8 -*-
"""
Created on Wed Apr  8 13:43:21 2020

@author: arpit.dhingra
"""

import os
import logging as logger 
import zipfile as zipper
import pandas as pd
import shutil
import tarfile as untar

try:
    def unzipFiles(list_of_files,file_path,dest_path,logpath,sheetname,ext_log_path):
        writeFile=open(logpath,'w+')
        lista=[]
        timestamp=0
        file_Content=[]
        df_file_logs=pd.DataFrame(columns=['Study ID/File','Timestamp','Filename','CRO','Extracted (Yes/No)'])
        for file in list_of_files:
            exception = False
            try:
                src_path=os.path.join(str(file_path),str(file))
                dst_path=os.path.join(str(dest_path),str(file).split('.')[0])
                dst_path.replace('\\','/')
                src_path.replace('\\','/')
                with zipper.ZipFile(src_path, 'r') as zip:
                    timestamp=getTimestamp(file)
                    lista=zip.namelist()
                    zip.extractall(dst_path)
                    if exception == False:
                        file_Content=getStudyIdSet(lista)
                        for item in file_Content:
                            df_file_logs=df_file_logs.append({'Study ID/File':item,'Timestamp':timestamp,'Filename':file,'CRO':sheetname,'Extracted (Yes/No)':'Yes'},ignore_index=True)    
                zip.close
            except RuntimeError:
                exception = True
                shutil.rmtree(dst_path)
                file_Content=getStudyIdSet(lista)
                for item in file_Content:
                    df_file_logs=df_file_logs.append({'Study ID/File':item,'Timestamp':timestamp,'Filename':file,'CRO':sheetname,'Extracted (Yes/No)':'No'},ignore_index=True)    
                writeFile.writelines(file)
                writeFile.writelines("\n")
        updateExtractionLogs(ext_log_path,df_file_logs)
        writeFile.close        
        
    def untarFiles(list_of_files,file_path,dest_path,logpath,sheetname,ext_log_path):
        writeFile=open(logpath,'w+')
        lista=[]
        timestamp=0
        file_Content=[]
        df_file_logs=pd.DataFrame(columns=['Study ID','Timestamp','Filename','CRO'])
        for file in list_of_files:
            exception = False
            try:
                src_path=os.path.join(str(file_path),str(file))
                dst_path=os.path.join(str(dest_path),str(file).split('.')[0])
                dst_path.replace('\\','/')
                src_path.replace('\\','/')
                with untar.TarFile(src_path, 'r') as tar:
                    timestamp=getTimestamp(file)
                    lista=tar.getnames()
                    tar.extractall(dst_path)
                    if exception == False:
                        file_Content=getStudyIdSet(lista)
                        for item in file_Content:
                            df_file_logs=df_file_logs.append({'Study ID/File':item,'Timestamp':timestamp,'Filename':file,'CRO':sheetname,'Extracted (Yes/No)':'Yes'},ignore_index=True)    
                tar.close
            except RuntimeError:
                exception = True
                shutil.rmtree(dst_path)
                file_Content=getStudyIdSet(lista)
                for item in file_Content:
                    df_file_logs=df_file_logs.append({'Study ID/File':item,'Timestamp':timestamp,'Filename':file,'CRO':sheetname,'Extracted (Yes/No)':'No'},ignore_index=True)    
                writeFile.writelines(file)
                writeFile.writelines("\n")
        updateExtractionLogs(ext_log_path,df_file_logs)
        writeFile.close        
        
    def getStudyIdSet(dir_list):
        studyList=[]
        for item in dir_list:
            item=item.split('/')[0]
            if(item.split('_')[0].isdigit()):
                studyList.append(item.split('_')[0])
            else:
                if (len(dir_list)) <= 3:
                    studyList.append(item.split('_')[0])
                else:
                    studyList.append('Dummy/Test File')
        #print('List --> ',studyList)
        #print('set --> ',set(studyList))
        return list(set(studyList))

    def checkIfStudyIdExists(studyId_list):
        for item in studyId_list:
            if item.isdigit():
                return True
        return False
        
    def updateExtractionLogs(filePath,logs_df):
        if (os.path.exists(filePath)):
            temp_df=pd.read_excel(filePath)
            column=temp_df.columns
            for item in column:
                if 'Unnamed' in item:
                    #print ('yes ',item)
                    temp_df=temp_df.drop(item,axis=1)
            updated_df=pd.concat([temp_df,logs_df],ignore_index=True,sort=False)
        else:
            updated_df=logs_df
        updated_df.to_excel(filePath,index=False)
        
    def getTimestamp(filename):
        filename=str(filename.split('.')[0])
        filename=filename.split('_')
        timestamp=filename[-1]
        return timestamp

    def getNewFolderName (path,studyId,timestamp):
        nameAndPathList=[]
        listOfDir=list(os.walk(path))
        path=str(listOfDir[0][0]).split("\\")
        string=""
        count = 0
        for element in path:
            if ('.zip' in element):
                element=element.replace('.zip','')
            if (count == 0):
                string = string+element
                count = count+1
            elif count>0 and element != '.zip':
                string=string+'/'+element
        nameAndPathList.append(string)
        nameAndPathList.append(studyId)
        nameAndPathList.append(studyId+'_'+str(timestamp))
        return nameAndPathList
    
    def removeOldStudyIdFromList(dirList):
        dictionary={}
        tempList=[]
        for element in dirList:
            tempList=str(element).split('_')
            if len(dictionary)<1 :
                dictionary[tempList[0]]=tempList[1]
            else:
                if tempList[0] in dictionary.keys():
                    if dictionary[tempList[0]] < tempList[1]:
                        dictionary[tempList[0]]=tempList[1]
                else:
                    dictionary[tempList[0]]=tempList[1]
        tempList.clear()
        for x,y in dictionary.items():
            if (str(x)+'_'+str(y) in dirList):
                tempList.append(str(x)+'_'+str(y))
            else:
                for element in dirList:
                    if str(element).split('_')[0] == str(x) and str(element).split('_')[1] == str(y):
                        tempList.append(str(element))
        print ("\nLatest Folders Extracted :-")
        for folder in tempList:
            print (folder)
        return tempList
        
except FileNotFoundError:
    logger.error("Got Error Here :- File Not Found... ",exc_info=True)
except NameError :
    logger.error("Name Error here --- ",exc_info=True)
except Exception :
    logger.error("General Exception --- ",exc_info=True)
