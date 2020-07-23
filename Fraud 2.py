#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
#import library pandas dan numpy

# In[2]:


order=pd.read_csv(r'C:\Users\rey\Documents\Shopee Project\20200601 Fraud Detetion\2. Prepared Data\orders.csv')
bank=pd.read_csv(r'C:\Users\rey\Documents\Shopee Project\20200601 Fraud Detetion\2. Prepared Data\bank_accounts.csv')
cc=pd.read_csv(r'C:\Users\rey\Documents\Shopee Project\20200601 Fraud Detetion\2. Prepared Data\credit_cards.csv')
device=pd.read_csv(r'C:\Users\rey\Documents\Shopee Project\20200601 Fraud Detetion\2. Prepared Data\devices.csv')
#masukkan tabel dalam file csv yang diberikan menggunakan library pandas 

# In[3]:


bdevice=pd.merge(order,device,how='left',left_on='buyer_userid',right_on='userid')
#gabungkan dua tabel order dan tabel device dengan referensi kolom buyer_userid pada tabel order dan userid pada tabel device

# In[4]:


del bdevice['userid']
#hapus kolom userid pada tabel bdevice

# In[5]:


bdevice=bdevice.rename(columns={'device':'bdevice'})
#ganti nama kolom device menjadi bdevice pada tabel bdevice agar dapat menbedakan device seller yang akan diterapkan pada syntax selanjutnya

# In[6]:


bdevice.bdevice=bdevice.bdevice.fillna('unknown B')
#ganti data yang bernilai 'NaN' pada kolom bdevice menjadi string 'unknown B'

# In[7]:


bsdevice=pd.merge(bdevice,device,how='left',left_on='seller_userid',right_on='userid')
#gabungkan tabel bdevice dengan device dengan kolom referensi seller_userid pada tabel bdevice dan userid pada tabel device

# In[8]:


del bsdevice['userid']
#hapus kolom userid pada tabel bsdevice

# In[9]:


bsdevice=bsdevice.rename(columns={'device':'sdevice'})
#ganti nama kolom device menjadi sdevice pada tabel bs device

# In[10]:


bsdevice.sdevice=bsdevice.sdevice.fillna('unknown S')
#ganti data yang bernilai 'NaN' pada kolom sdevice menjadi string 'unknown s'
#oleh karena itu sudah dapat dibandingkan device yang dimiliki oleh byer dan device yang dimiliki oleh seller
#jika device yang dimiliki seller dan device yang dimiliki buyer dapat disimpulkan seller tersebut melakukan order brushing
# In[11]:


bsdevice['dsama']=np.where(bsdevice.bdevice==bsdevice.sdevice,1,0)
#menggunakan fungsi numpy.where sebagai tools untuk melakukan kondisi logic (logical condition)
#jika terdapat device yang sama pada buyer dan seller, akan diberikan nilai 1, jika tidak bernilai 0
# In[12]:


cek=bsdevice.groupby(['orderid','buyer_userid','seller_userid']).dsama.sum()
#kelompokkan orderid, buyer_userid, dan seller_userid agar dapat dilihat pertambahan dari jumlah kemungkinan device yang sama
#antara buyer dan seller

# In[13]:


proses1=cek[cek==0].reset_index()
#filter tabel dengan order_id dengan device yang sama yang akan menyisakan kumpulan order id yang belum terbukti melakukan order brushing

# In[14]:


del proses1['dsama']
#hapus kolom bernama dsama pada tabel proses1

# In[15]:


bcc=pd.merge(proses1,cc,how='left',left_on='buyer_userid',right_on='userid')
#gabungkan dua tabel proses1 dan tabel cc dengan referensi kolom buyer_userid pada tabel proses1 dan userid pada tabel cc

# In[16]:


del bcc['userid']
#hapus kolom userid pada tabel bcc

# In[17]:


bcc=bcc.rename(columns={'credit_card':'bcredit_card'})
#ganti nama kolom device menjadi credit_card pada tabel bcredit_card agar dapat menbedakan credit_card seller yang akan diterapkan pada syntax selanjutnya


# In[18]:


bcc.bcredit_card=bcc.bcredit_card.fillna('unknown B')
#ganti data yang bernilai 'NaN' pada kolom bcredit_card menjadi string 'unknown B'

# In[19]:


bscc=pd.merge(bcc,cc,how='left',left_on='seller_userid',right_on='userid')
#gabungkan tabel bcc dengan cc dengan kolom referensi seller_userid pada tabel bcc dan userid pada tabel cc

# In[20]:


del bscc['userid']
#hapus kolom userid pada tabel bscc

# In[21]:


bscc=bscc.rename(columns={'credit_card':'scredit_card'})
#ganti nama kolom credit_card menjadi scredit_card pada tabel bscc

# In[22]:


bscc.scredit_card=bscc.scredit_card.fillna('unknown S')
#ganti data yang bernilai 'NaN' pada kolom bscc menjadi string 'unknown s'
#oleh karena itu sudah dapat dibandingkan credit_card yang dimiliki oleh buyer dan credit_card yang dimiliki oleh seller
#jika credit_card yang dimiliki seller dan credit_card yang dimiliki buyer dapat disimpulkan seller tersebut melakukan order brushing

# In[23]:


bscc['csama']=np.where(bscc.bcredit_card==bscc.scredit_card,1,0)
#menggunakan fungsi numpy.where sebagai tools untuk melakukan kondisi logic (logical condition)
#jika terdapat credit_card yang sama pada buyer dan seller, akan diberikan nilai 1, jika tidak bernilai 0

# In[24]:


cek1=bscc.groupby(['orderid','buyer_userid','seller_userid']).csama.sum()
#kelompokkan orderid, buyer_userid, dan seller_userid agar dapat dilihat pertambahan dari jumlah kemungkinan credit_card yang sama
#antara buyer dan seller

# In[25]:


proses2=cek1[cek1==0].reset_index()
#filter tabel dengan order_id dengan credit_card yang sama yang akan menyisakan kumpulan order id yang belum terbukti melakukan order brushing

# In[26]:


del proses2['csama']
#hapus kolom bernama csama pada tabel proses2

# In[27]:


bbank=pd.merge(proses2,bank,how='left',left_on='buyer_userid',right_on='userid')
#gabungkan dua tabel proses2 dan tabel bank dengan referensi kolom buyer_userid pada tabel proses2 dan userid pada tabel bank

# In[28]:


del bbank['userid']
#hapus kolom userid pada tabel bbank

# In[29]:


bbank=bbank.rename(columns={'bank_account':'bbank_account'})
#ganti nama kolom bank_account menjadi bank_account pada tabel bbank agar dapat menbedakan rekening seller yang akan diterapkan pada syntax selanjutnya

# In[30]:


bbank.bbank_account=bbank.bbank_account.fillna('unknown B')
#ganti data yang bernilai 'NaN' pada kolom bbank menjadi string 'unknown B'

# In[31]:


bsbank=pd.merge(bbank,bank,how='left',left_on='seller_userid',right_on='userid')
#gabungkan tabel bbank dengan bank dengan kolom referensi seller_userid pada tabel bbank dan userid pada tabel bank

# In[32]:


del bsbank['userid']
#hapus kolom userid pada tabel bsbank

# In[33]:


bsbank=bsbank.rename(columns={'bank_account':'sbank_account'})
#ganti nama kolom bank_account menjadi sbank_account pada tabel bsbank

# In[34]:


bsbank.sbank_account=bsbank.sbank_account.fillna('unknown S')
#ganti data yang bernilai 'NaN' pada kolom bsbank menjadi string 'unknown s'
#oleh karena itu sudah dapat dibandingkan rekening yang dimiliki oleh buyer dan rekening yang dimiliki oleh seller
#jika rekening yang dimiliki seller dan rekening yang dimiliki buyer dapat disimpulkan seller tersebut melakukan order brushing

# In[35]:


bsbank['bsama']=np.where(bsbank.bbank_account==bsbank.sbank_account,1,0)
#menggunakan fungsi numpy.where sebagai tools untuk melakukan kondisi logic (logical condition)
#jika terdapat rekening yang sama pada buyer dan seller, akan diberikan nilai 1, jika tidak bernilai 0

# In[36]:


cek2=bsbank.groupby(['orderid','buyer_userid','seller_userid']).bsama.sum()
#kelompokkan orderid, buyer_userid, dan seller_userid agar dapat dilihat pertambahan dari jumlah kemungkinan rekening yang sama
#antara buyer dan seller

# In[37]:


proses3=cek2[cek2==0].reset_index()
#filter tabel dengan order_id dengan rekening yang sama yang akan menyisakan kumpulan order id yang belum terbukti melakukan order brushing

# In[38]:


del proses3['bsama']
#hapus kolom bernama bsama pada tabel proses3

# In[39]:


proses3['is_fraud']=0
#buat suatu kolom pada proses 3 bernama is_fraud

# In[40]:


answer=pd.merge(order.orderid,proses3.is_fraud,how='left',left_on=order.orderid,right_on=proses3.orderid)
#gabungkan dua tabel pada tabel order dengan kolom orderid dan tabel proses3 dengan referensi kolom orderid pada tabel order dan orderid pada tabel proses3
#dikarenakan menggunakan fungsi left pada merge, hal tersebut membuat orderid yang terindikasi melakukan order brushing
#akan bernilai 'NaN'
# In[41]:


answer.is_fraud=answer.is_fraud.fillna(1)
#mengganti nilai 'NaN' pada kolom is_fraud menjadi 1

# In[42]:


answer.is_fraud=answer.is_fraud.astype(int)
#mengganti tipe object pada kolom is_fraud sebagai integer

# In[43]:


del answer['key_0']
#hapus kolom key_0 pada tabel answer

# In[44]:


answer
#tampilkan sebagian data tabel answer

# In[45]:


#answer.to_csv('fraud_answer.csv')
#export table dalam bentuk csv bernama fraud_answer




