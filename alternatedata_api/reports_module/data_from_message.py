import pickle
import nltk
import re
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
import pandas as pd
import numpy as np
from datetime import datetime


nltk.download('stopwords')

class message_analysis:
    def load_models(self):
        self.classification_model=pickle.load(open('alternatedata_api/reports_module/save_objs/finalized_model.sav','rb'))
        self.load_tifdfvec=pickle.load(open('alternatedata_api/reports_module/save_objs/tifdfvec.obj','rb'))
        self.load_tfid=pickle.load(open('alternatedata_api/reports_module/save_objs/tfid.obj','rb'))


    def string_preprocessing(self,series):

        courpus=[]
        ps=PorterStemmer()
        for i in range(0,len(series)):
            try:
                review=re.sub('[^a-zA-Z]',' ',series[i])  #select only text from the dataset
                review=review.lower()
                review=review.split()
                ps=PorterStemmer()
                review=[ps.stem(word) for word in review if not word in set(stopwords.words('english'))]
                review=" ".join(review)
                courpus.append(review)
            except:
                courpus.append(" ")
        return courpus

    def filtersystem_msg(self,dataframe):
        system_msg=dataframe[dataframe['send_name'].str.contains('[a-zA-Z]+')]
        test_data=system_msg['body_messages']
        message_date=system_msg['message_date']
        return test_data,message_date

    def predict_labels(self,dataframe):
        self.load_models()
        test_data,message_date=self.filtersystem_msg(dataframe)
        courpus=self.string_preprocessing(test_data.tolist())
        tifdfvec=self.load_tifdfvec.transform(courpus)
        tfid=self.load_tfid.transform(tifdfvec)
        label=self.classification_model.predict(tfid)
        result=pd.DataFrame({'message_date':message_date,'x':test_data.tolist(),'y':label})
        self.result=result
        bank_message=result[result['y']==2]
        return bank_message

    def bank_data2json(self,bank_message):
        transtion=[]
        for text in range(len(bank_message)):
        #     print(re.findall('X+\d+|x+\d+',bank_message.iloc[text,1]),bank_message.iloc[text,1][:100])
            line=bank_message.iloc[text,1].lower()

            if 'credited by' in line:
                idx=line.find('credited')
                bal_idx=line.find('bal')
                bal_line=line[bal_idx:]
                bal_line=bal_line.replace(",","")
                # print(bal_line)
                try:
                    bal=re.findall('\d+[.\d+]*',bal_line)[0]
                except:
                    bal=np.nan
                line=line[idx:]
                line=line.replace(",","")
        #         re.findall('\d+',line)
        #         print('credit',re.findall('\d+[.\d+]*',line)[0])
                ac_info=re.findall('X+\d+|x+\d+',bank_message.iloc[text,1])
                transtion.append({'credited':re.findall('\d+[.\d+]*',line)[0],'ac_info':ac_info,'bal':bal})
            elif 'credited to' in line:
                idx =line.find('credited')
                bal_idx=line.find('bal')
                bal_line=line[bal_idx:]
                bal_line=bal_line.replace(",","")
                try:
                    bal=re.findall('\d+[.\d+]*',bal_line)[0]
                except:
                    bal=np.nan
                line=line[:idx]
                line=line.replace(",","")
        #         print('credit',re.findall('\d+[.\d+]*',line)[0])
                ac_info=re.findall('X+\d+|x+\d+',bank_message.iloc[text,1])
                transtion.append({'credited':re.findall('\d+[.\d+]*',line)[0],'ac_info':ac_info,'bal':bal})

            elif 'debited by' in line:
                idx =line.find('debited')
                bal_idx=line.find('bal')
                bal_line=line[bal_idx:]
                bal_line=bal_line.replace(",","")
                try:
                    bal=re.findall('\d+[.\d+]*',bal_line)[0]
                except:
                    bal=np.nan
                line=line[idx:]
                line=line.replace(",","")
        #         print('debited',re.findall('\d+[.\d+]*',line)[0])
                ac_info=re.findall('X+\d+|x+\d+',bank_message.iloc[text,1])
                transtion.append({'debited':re.findall('\d+[.\d+]*',line)[0],'ac_info':ac_info,'bal':bal})
            elif 'atm' in line:
                idx =line.find('atm')
                bal_idx=line.find('bal')
                bal_line=line[bal_idx:]
                bal_line=bal_line.replace(",","")
                try:
                    bal=re.findall('\d+[.\d+]*',bal_line)[0]
                except:
                    bal=np.nan
                line=line[idx:]
                line=line.replace(",","")
        #         print("atm debited",re.findall("rs \d+[.\d+]*|rs. \d+[.\d+]*",line)[0].split()[1])
                ac_info=re.findall('X+\d+|x+\d+',bank_message.iloc[text,1])
                transtion.append({"atm debited":re.findall("rs \d+[.\d+]*|rs. \d+[.\d+]*",line)[0].split()[1],'card_info':ac_info,'bal':bal})
            else:
                transtion.append({})


        array=np.concatenate([bank_message['message_date'].values.reshape(-1,1),pd.DataFrame(transtion).values],axis=1)
        column=list(pd.DataFrame(transtion).columns)
        column.insert(0,'message_date')
        data=pd.DataFrame(array,columns=column)
        data1=data.copy()
        data1['message_date']=pd.to_datetime(data1['message_date'])
        data1['message_date']=data1['message_date'].dt.strftime('%B-%Y')
        groups=data1.groupby(data1['message_date'])
        self.data1=data1
        json_output={}
        for name, group in groups:
        #     print(name)
            group_dict=data.iloc[group['message_date'].index].T.to_dict()
        #     print(list(group_dict.values()))
            total_credit=sum([float(c['credited']) for c in list(group_dict.values()) if c['credited'] is not np.nan])
            total_debit=sum([float(c['debited']) for c in list(group_dict.values()) if c['debited'] is not np.nan])
            total_atm_debit=sum([float(c['atm debited']) for c in list(group_dict.values()) if c['atm debited'] is not np.nan])
            json_output[name]={'total_credited':total_credit,'total_debited':total_debit,'total_atm_debit':total_atm_debit,'data':list(group_dict.values())}

        return json_output

    def classify_message(self):
        self.result['y']=self.result['y'].map({0:'install app',1:'spam',2:'bank', 3:'other',4:'network'})
        groups=self.result.groupby(self.result['y'])
        json_data={}
        for name,group in groups:
            json_data[name]=dict(zip(group['message_date'].to_list(),group['x'].to_list()))
        return json_data

    def avg_monthly_bal(self):
        self.data1
        json_data={}
        ac_info=[]
        for a in self.data1['ac_info']:
            if type(a) == list and len(a)>0:
        #         print(a[0])
                ac_info.append(a[0])
            else:
                ac_info.append(np.nan)
        self.data1['ac_info']=ac_info
        unique_ac=self.data1['ac_info'].unique().tolist()
        # print(unique_ac)
        for a in unique_ac:
            data={}
            dataforac1=self.data1[self.data1['ac_info']==a]
            groups=dataforac1.groupby(dataforac1['message_date'])
            for name,group in groups:
                data[name]=np.mean([float(v) for v in group['bal'] if v is not np.nan])
            json_data[a]=data
        return json_data

    def newformat_function(self):
        ds=self.result
        bank_data=ds[ds['y']==2]
        lines=bank_data['x'].to_list()
        message_date=bank_data['message_date'].to_list()

        data=[]
        transtion=[]
        for l in range(len(lines)):
            ac_no=re.findall('X+\d+|x+\d+',lines[l])
        #     print(ac_no,l,lines[l])
            date=message_date[l]
            if len(ac_no)==0:
                #<---------------if a/c is not detect------------
        #         print("paytm",re.findall('\d{10}',lines[l]),"-------",lines[l])
                receiver_paytmno=re.findall('[1-9]\d{9} ',lines[l])
                if len(receiver_paytmno)!=0:
                    #<-----extract paytmno----------------
                    if len(receiver_paytmno)==0:
                        receiver_paytmno=np.nan
                    else:
                        receiver_paytmno=receiver_paytmno[0]

                    #<--------------amount---------------
                    amount=re.findall('\d+[.\d+]*',lines[l])[0]

                    #<------------available amount----------
                    bal_idx=lines[l].lower().find('bal')
                    bal_line=lines[l][bal_idx:]
                    bal_line=bal_line.replace(",","")
                    try:
                        bal=re.findall('\d+[.\d+]*',bal_line)[0]
                    except:
                        bal=np.nan

                    data.append({'receiver paytm no':receiver_paytmno,'date':date,'paid amount':amount,'available_amount':bal,'type':'paytm'})
                else:
                    #----------------if no account a/c detect---------------
                    line=lines[l].lower()
                    # print(line)
                    if 'credited by' in line:
                        idx=line.find('credited')
                        bal_idx=line.find('bal')
                        bal_line=line[bal_idx:]
                        bal_line=bal_line.replace(",","")
            #             print(bal_line)
                        try:
                            bal=re.findall('\d+[.\d+]*',bal_line)[0]
                        except:
                            bal=np.nan
                        line=line[idx:]
                        line=line.replace(",","")
            #             ac_info=re.findall('X+\d+|x+\d+',bank_message.iloc[text,1])
                        data.append({'credited':re.findall('\d+[.\d+]*',line)[0],'available_amount':bal,'date':date,'type':'other'})
                    elif 'credited to' in line:
                        idx =line.find('credited')
                        bal_idx=line.find('bal')
                        bal_line=line[bal_idx:]
                        bal_line=bal_line.replace(",","")
                        try:
                            bal=re.findall('\d+[.\d+]*',bal_line)[0]
                        except:
                            bal=np.nan
                        line=line[:idx]
                        line=line.replace(",","")
                #         print('credit',re.findall('\d+[.\d+]*',line)[0])
            #             ac_info=re.findall('X+\d+|x+\d+',bank_message.iloc[text,1])
                        data.append({'credited':re.findall('\d+[.\d+]*',line)[0],'available_amount':bal,'date':date,'type':'other'})

                    elif 'debited by' in line:
                        idx =line.find('debited')
                        bal_idx=line.lower().find('bal')
                        bal_line=line[bal_idx:]
                        bal_line=bal_line.replace(",","")
                        try:
                            bal=re.findall('\d+[.\d+]*',bal_line)[0]
                        except:
                            bal=np.nan
                        line=line[idx:]
                        line=line.replace(",","")
                #         print('debited',re.findall('\d+[.\d+]*',line)[0])
            #             ac_info=re.findall('X+\d+|x+\d+',bank_message.iloc[text,1])
                        data.append({'debited':re.findall('\d+[.\d+]*',line)[0],'available_amount':bal,'date':date,'type':'other'})
                    elif 'atm' in line:
                        idx =line.find('atm')
                        bal_idx=line.lower().find('bal')
                        bal_line=line[bal_idx:]
                        bal_line=bal_line.replace(",","")
                        try:
                            bal=re.findall('\d+[.\d+]*',bal_line)[0]
                        except:
                            bal=np.nan
                        line=line[idx:]
                        line=line.replace(",","")
                #         print("atm debited",re.findall("rs \d+[.\d+]*|rs. \d+[.\d+]*",line)[0].split()[1])
            #             ac_info=re.findall('X+\d+|x+\d+',bank_message.iloc[text,1])
                        data.append({"atm debited":re.findall("rs \d+[.\d+]*|rs. \d+[.\d+]*",line)[0].split()[1],'available_amount':bal,'date':date,'type':'other'})
                    elif 'bal' in line:
                        bal_idx=line.lower().find('bal')
                        bal_line=line[bal_idx:]
                        bal_line=bal_line.replace(",","")
                        try:
                            bal=re.findall('\d+[.\d+]*',bal_line)[0]
                        except:
                            bal=np.nan
                        data.append({"available_amount":bal,'date':date,'type':'other'})
                    else:
                        data.append({})
        #                 print(line)

            else:
                line=lines[l].lower()
        #         print(line)
                if 'credited by' in line:
                    idx=line.find('credited')
                    bal_idx=line.find('bal')
                    bal_line=line[bal_idx:]
                    bal_line=bal_line.replace(",","")
        #             print(bal_line)
                    try:
                        bal=re.findall('\d+[.\d+]*',bal_line)[0]
                    except:
                        bal=np.nan
                    if 'upi' in line:
                        trans_type='UPI'
                    elif 'neft' in line:
                        trans_type='NEFT'
                    else:
                        trans_type='other'
                    # print(re.findall('\d+[.\d+]*',line))

                    line=line[idx:]
                    line=line.replace(",","")

                    temp_output={'credited':re.findall('\d+[.\d+]*',line)[0],'available_amount':bal,'date':date,'type':trans_type}
                    if 'a/c' in line:
                        ac_number={'ac_number':ac_no[0]}
                    elif 'card' in line:
                        ac_number={'card info':ac_no[0]}

                    temp_output.update(ac_number)

        #             ac_info=re.findall('X+\d+|x+\d+',bank_message.iloc[text,1])
                    data.append(temp_output)
                elif 'credited to' in line:
                    idx =line.find('credited')
                    bal_idx=line.find('bal')
                    bal_line=line[bal_idx:]
                    bal_line=bal_line.replace(",","")
                    try:
                        bal=re.findall('\d+[.\d+]*',bal_line)[0]
                    except:
                        bal=np.nan

                    if 'upi' in line:
                        trans_type='UPI'
                    elif 'neft' in line:
                        trans_type='NEFT'
                    else:
                        trans_type='other'

                    line=line[:idx]
                    line=line.replace(",","")
                    temp_output={'credited':re.findall('\d+[.\d+]*',line)[0],'available_amount':bal,'date':date,'type':trans_type}
                    if 'a/c' in line:
                        ac_number={'ac_number':ac_no[0]}
                    elif 'card' in line:
                        ac_number={'card info':ac_no[0]}

                    temp_output.update(ac_number)

            #         print('credit',re.findall('\d+[.\d+]*',line)[0])
        #             ac_info=re.findall('X+\d+|x+\d+',bank_message.iloc[text,1])
                    data.append(temp_output)

                elif 'debited by' in line:
                    idx =line.find('debited')
                    bal_idx=line.lower().find('bal')
                    bal_line=line[bal_idx:]
                    bal_line=bal_line.replace(",","")
                    try:
                        bal=re.findall('\d+[.\d+]*',bal_line)[0]
                    except:
                        bal=np.nan

                    if 'upi' in line:
                        trans_type='UPI'
                    elif 'neft' in line:
                        trans_type='NEFT'
                    else:
                        trans_type='other'

                    line=line[idx:]
                    line=line.replace(",","")
                    temp_output={'debited':re.findall('\d+[.\d+]*',line)[0],'available_amount':bal,'date':date,'type':trans_type}
                    if 'a/c' in line:
                        ac_number={'ac_number':ac_no[0]}
                    elif 'card' in line:
                        ac_number={'card info':ac_no[0]}

                    temp_output.update(ac_number)

            #         print('debited',re.findall('\d+[.\d+]*',line)[0])
        #             ac_info=re.findall('X+\d+|x+\d+',bank_message.iloc[text,1])
                    data.append(temp_output)
                elif 'atm' in line:
                    idx =line.find('atm')
                    bal_idx=line.lower().find('bal')
                    bal_line=line[bal_idx:]
                    bal_line=bal_line.replace(",","")
                    try:
                        bal=re.findall('\d+[.\d+]*',bal_line)[0]
                    except:
                        bal=np.nan
                    line=line[idx:]
                    line=line.replace(",","")
                    temp_output={"atm debited":re.findall("rs \d+[.\d+]*|rs. \d+[.\d+]*",line)[0].split()[1],'available_amount':bal,'date':date,'type':'atm withdraw'}
                    if 'a/c' in line:
                        ac_number={'ac_number':ac_no[0]}
                    elif 'card' in line:
                        ac_number={'card info':ac_no[0]}

                    temp_output.update(ac_number)


                    data.append(temp_output)
                elif 'bal' in line:
                    bal_idx=line.lower().find('bal')
                    bal_line=line[bal_idx:]
                    bal_line=bal_line.replace(",","")
                    try:
                        bal=re.findall('\d+[.\d+]*',bal_line)[0]
                    except:
                        bal=np.nan

                    temp_output={"available_amount":bal,'date':date,'type':'checked bal'}
                    if 'a/c' in line:
                        ac_number={'ac_number':ac_no[0]}
                    elif 'card' in line:
                        ac_number={'card info':ac_no[0]}
                    temp_output.update(ac_number)
                    data.append(temp_output)
                else:
                    data.append({})
        dataset=pd.DataFrame(data)
        bank_ac_list=dataset['ac_number'].value_counts()
        bank_ac_list=bank_ac_list.index.to_list()
        final_output_json={}
        for ac_number in bank_ac_list:
            filter_dataset=dataset[dataset['ac_number']==ac_number]
            type_list=filter_dataset['type'].value_counts()
            type_list=type_list.index.to_list()
            type_name_output={}
            for type_name in type_list:
                filter_dataset1=filter_dataset[filter_dataset['type']==type_name]
                filter_dataset1=filter_dataset1.dropna(how='all')
                date=pd.to_datetime(filter_dataset1['date'])
                date=date.dt.strftime('%B-%Y')
                filter_dataset1['month-year']=date
                groups=filter_dataset1.groupby(filter_dataset1['month-year'])
                json_output={}
                for name,group in groups:
                    group_dict=group.T.to_dict()
                    total_credit=sum([float(c['credited']) for c in list(group_dict.values()) if c['credited'] is not np.nan])
                    total_debit=sum([float(c['debited']) for c in list(group_dict.values()) if c['debited'] is not np.nan])
                    total_atm_debit=sum([float(c['atm debited']) for c in list(group_dict.values()) if c['atm debited'] is not np.nan])
                    json_output[name]={'total_credited':total_credit,'total_debited':total_debit,'total_atm_debit':total_atm_debit,'data':list(group_dict.values())}

        #         print(filter_dataset1)
                temp_data=pd.DataFrame(json_output).T
                index=pd.DataFrame(json_output).T.index
                datedata=[datetime.strptime(i,'%B-%Y') for i in index]
                temp_data['datetime_columns']=datedata
                temp_data=temp_data.sort_values(by="datetime_columns")
                temp_data=temp_data.drop('datetime_columns',axis=1)
                json_output=temp_data.T.to_dict()

                type_name_output[type_name]=json_output
        #     print(type_list)
            final_output_json[ac_number]=type_name_output
        return final_output_json

# obj=message_analysis()
# dataframe=pd.read_csv("mail_and_messages.csv")
# data=obj.predict_labels(dataframe)
# json=obj.bank_data2json(data)
