import re
import pandas as pd
from os.path import exists,join
from os import walk

folder_Path=r'../../New Folder'
row=0
dict={}
end_file_path=r'/New Folder/CT File.csv'
date='2021-05-01'
if exists(end_file_path):
    df=pd.read_csv(end_file_path)
    row=len(df.index)
    date = df['Last Occured On'].max()
    print (date)
    row-=1
else:
    column = ['Sr. No.' ,'Transformation' ,'Column With Issue' ,'Non-CT Value' ,'File with Issue' ,'Last Occured On','Source File']
    df = pd.DataFrame(columns=column)

list_files=walk(folder_Path)
list_files=list(list_files)
list_files=list_files[0][-1]
for file in list_files:
    file_Path=join(folder_Path,file)
    file=file.split('.')[-1]
    if (file.lower() == 'log'):
        with open(file_Path,'r') as logs:
            for log_line in logs:
                if (log_line.split(' ')[0] >= date and '"type": "NON_CONFORMING_TERMS"' in log_line):
                    #print ('True')
                    #print(type(log_line),'\n',log_line,'\n',log_line.split(' ')[0])
                    word=re.search(r'"transform": (\S+)' ,log_line)
                    word=word.group(1).replace('",','').replace('"','')
                    #print('Transformation - ' ,word)
                    transformation=word
                    word = re.search(r'The following values in column (\S+)' ,log_line)
                    word=word.group(1).replace('",','')
                    #print ('Column - ',word)
                    column=word
                    word = re.search(r'do not conform to CT: (\S+)' ,log_line)
                    word=word.group(1).replace('",','')
                    #print('Value - ' ,word)
                    valueSet = word
                    word = re.search(r'"fileidentifier": (\S+)' ,log_line)
                    word=word.group(1).replace('",','').replace('"','')
                    #print('File - ' ,word)
                    file=word
                    date = log_line.split(' ')[0]
                    dict[valueSet]=[transformation,column,valueSet,file,date,file_Path]
        logs.close()

for key in dict.keys():
    if exists(end_file_path) and (dict[key][2] not in df['Non-CT Value'].values):
        df.at[row ,'Sr. No.'] = row + 1
        df.at[row,'Transformation'] = dict[key][0]
        df.at[row,'Column With Issue'] = dict[key][1]
        df.at[row,'Non-CT Value'] = dict[key][2]
        df.at[row ,'File with Issue'] = dict[key][3]
        df.at[row,'Last Occured On'] = dict[key][4]
        df.at[row ,'Source File'] = dict[key][4]
    elif not exists(end_file_path):
        df.at[row ,'Sr. No.'] = row + 1
        df.at[row ,'Transformation'] = dict[key][0]
        df.at[row ,'Column With Issue'] = dict[key][1]
        df.at[row ,'Non-CT Value'] = dict[key][2]
        df.at[row ,'File with Issue'] = dict[key][3]
        df.at[row ,'Last Occured On'] = dict[key][4]
        df.at[row ,'Source File'] = dict[key][5]
    row+=1
df.to_csv(end_file_path,index=False)

