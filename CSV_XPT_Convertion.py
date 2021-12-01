import pandas as pd
import xport
import sys

def check():
    n = len(sys.argv)
    if n > 1:
        filePath = sys.argv[1]
    else:
        filePath = input('Please provide File to Convert -: ')

    filePath=filePath.replace("'",'')
    filePath = filePath.replace('"' ,'')

    if (filePath.split('.')[-1]).lower() == 'csv':
        csv_to_xpt(filePath)
    elif (filePath.split('.')[-1]).lower() == 'xpt':
        xpt_to_csv(filePath)
    else:
        print ('Wrong File Provided or Incomplete Path provided.\nPlease provide Complete File Path with Extension')

def xpt_to_csv(path):
    df=pd.read_sas(path, format = "xport",encoding="utf-8")
    csv_path=path.replace('.xpt','.csv')
    df.to_csv(csv_path,index=False)

def csv_to_xpt(path):
    xpt_path=path.replace('.csv','.xpt')
    df = pd.read_csv(path)
    data = df.T.to_dict().values()
    with open(xpt_path, 'wb') as f:
        xport.from_rows(data, f)


if __name__ != '__main__':
    pass
else:
    check()
