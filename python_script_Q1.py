import math
import numpy as np
import pandas as pd
import sys
from os import path
from pathlib import Path

#file_name1=Path(sys.argv[0])
#file_name2=Path(sys.argv[1])
#file_name3=Path(sys.argv[2])
listOfLines =[] 
wdf={}
with open ("/home/maria_dev/Assignment/Q1_part-r-00000_PH1.txt" , "r") as myfile:
	for line in myfile:
		listOfLines.append(line.strip())
for i in range(len(listOfLines)):
	word=str(listOfLines[i]).split(',')[0]
	doc=((str(listOfLines[i]).split(',')[1]).split('\t')[0])
	fre=((str(listOfLines[i]).split(',')[1]).split('\t')[1])
	key=word+'_'+doc
	wdf[key]=fre
dc={}
with open ("/home/maria_dev/Assignment/Q1_part-r-00000_PH2.txt", "r") as myfile1:
	for line in myfile1:
		n=line.strip().split('\t')
		dc[n[0]]=n[1]
wd={}
with open ("/home/maria_dev/Assignment/Q1_part-r-00000_PH3.txt", "r") as myfile2:
	for line in myfile2:
		n=line.strip().split('\t')
		wd[n[0]]=n[1]
L=list(wd.keys())
M=list(dc.keys())
N=list(wdf.keys())
p=len(L)
q=len(M)
k=np.zeros([p,q],dtype=float)
res=0
for x,i in zip(L,range(len(L))):
	for y,j in zip(M,range(len(M))):        
		key=x+'_'+y
		tf_nr=wdf.get(key,0)
		tf_dr=dc[y]
		tf=int(tf_nr)/int(tf_dr)
		idf_nr=len(M)
		idf_dr=wd[x]
		idf=int(idf_nr)/int(idf_dr)
		res=tf*(np.log10(idf))
		k[i][j]=res
TFIDF=pd.DataFrame(data=k,index=L,columns=M)
for i,j in zip(range(len(TFIDF.columns)),range(q)):
	list1=[]
	sorted_TFIDF=TFIDF.sort_values(TFIDF.columns[i],ascending=False)
	print("The Top 18 terms with highest TFIDF values in the document  " +str(TFIDF.columns[i]) + ' are:' )
	print(sorted_TFIDF.iloc[:18,i])   
