#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np


# In[2]:


order=pd.read_csv(r'C:\Users\rey\Documents\Shopee Project\20200601 Fraud Detetion\2. Prepared Data\orders.csv')
bank=pd.read_csv(r'C:\Users\rey\Documents\Shopee Project\20200601 Fraud Detetion\2. Prepared Data\bank_accounts.csv')
cc=pd.read_csv(r'C:\Users\rey\Documents\Shopee Project\20200601 Fraud Detetion\2. Prepared Data\credit_cards.csv')
device=pd.read_csv(r'C:\Users\rey\Documents\Shopee Project\20200601 Fraud Detetion\2. Prepared Data\devices.csv')


# In[3]:


bdevice=pd.merge(order,device,how='left',left_on='buyer_userid',right_on='userid')


# In[4]:


del bdevice['userid']


# In[5]:


bdevice=bdevice.rename(columns={'device':'bdevice'})


# In[6]:


bdevice.bdevice=bdevice.bdevice.fillna('unknown B')


# In[7]:


bsdevice=pd.merge(bdevice,device,how='left',left_on='seller_userid',right_on='userid')


# In[8]:


del bsdevice['userid']


# In[9]:


bsdevice=bsdevice.rename(columns={'device':'sdevice'})


# In[10]:


bsdevice.sdevice=bsdevice.sdevice.fillna('unknown S')


# In[11]:


bsdevice['dsama']=np.where(bsdevice.bdevice==bsdevice.sdevice,1,0)


# In[12]:


cek=bsdevice.groupby(['orderid','buyer_userid','seller_userid']).dsama.sum()


# In[13]:


proses1=cek[cek==0].reset_index()


# In[14]:


del proses1['dsama']


# In[15]:


bcc=pd.merge(proses1,cc,how='left',left_on='buyer_userid',right_on='userid')


# In[16]:


del bcc['userid']


# In[17]:


bcc=bcc.rename(columns={'credit_card':'bcredit_card'})


# In[18]:


bcc.bcredit_card=bcc.bcredit_card.fillna('unknown B')


# In[19]:


bscc=pd.merge(bcc,cc,how='left',left_on='seller_userid',right_on='userid')


# In[20]:


del bscc['userid']


# In[21]:


bscc=bscc.rename(columns={'credit_card':'scredit_card'})


# In[22]:


bscc.scredit_card=bscc.scredit_card.fillna('unknown S')


# In[23]:


bscc['csama']=np.where(bscc.bcredit_card==bscc.scredit_card,1,0)


# In[24]:


cek1=bscc.groupby(['orderid','buyer_userid','seller_userid']).csama.sum()


# In[25]:


proses2=cek1[cek1==0].reset_index()


# In[26]:


del proses2['csama']


# In[27]:


bbank=pd.merge(proses2,bank,how='left',left_on='buyer_userid',right_on='userid')


# In[28]:


del bbank['userid']


# In[29]:


bbank=bbank.rename(columns={'bank_account':'bbank_account'})


# In[30]:


bbank.bbank_account=bbank.bbank_account.fillna('unknown B')


# In[31]:


bsbank=pd.merge(bbank,bank,how='left',left_on='seller_userid',right_on='userid')


# In[32]:


del bsbank['userid']


# In[33]:


bsbank=bsbank.rename(columns={'bank_account':'sbank_account'})


# In[34]:


bsbank.sbank_account=bsbank.sbank_account.fillna('unknown S')


# In[35]:


bsbank['bsama']=np.where(bsbank.bbank_account==bsbank.sbank_account,1,0)


# In[36]:


cek2=bsbank.groupby(['orderid','buyer_userid','seller_userid']).bsama.sum()


# In[37]:


proses3=cek2[cek2==0].reset_index()


# In[38]:


del proses3['bsama']


# In[39]:


proses3['is_fraud']=0


# In[40]:


answer=pd.merge(order.orderid,proses3.is_fraud,how='left',left_on=order.orderid,right_on=proses3.orderid)


# In[41]:


answer.is_fraud=answer.is_fraud.fillna(1)


# In[42]:


answer.is_fraud=answer.is_fraud.astype(int)


# In[43]:


del answer['key_0']


# In[44]:


answer


# In[45]:


#answer.to_csv('fraud_answer.csv')


# In[46]:


len(answer.orderid)-len(answer[answer.is_fraud==1])


# In[ ]:




