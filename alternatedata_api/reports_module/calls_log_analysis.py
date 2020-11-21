import pandas as pd
from datetime import datetime
import statistics
import numpy as np
import datetime as dt
import pickle


class calls_log_report:
    #read dataframe
    def read_data(self,path):
        self.data=pd.read_csv(path)
        return self.data
    #Call answer rate - calls picked to calls received
    def callanswerrate(self):
        calltypevaluecounts=dict(self.data['call_type'].value_counts())
        total_call=sum(calltypevalues.values())
        incoming_call=calltypevaluecounts['INCOMING']
        answerrate=incoming_call/total_call*100
        return answerrate

    #Average calls per day - incoming and outgoing both
    def avgcallperday(self):
        date=[]
        time=[]
        date_time=[]
        for d in self.data['date'].tolist():
            datedata=datetime.strptime(d,'%a %b %d %H:%M:%S %Z%z %Y')
            date.append(datedata.strftime("%d/%m/%Y"))
            time.append(datedata.strftime("%H:%M:%S"))
            date_time.append(datedata.strftime("%d/%m/%Y %H:%M:%S"))
        date_time={'date':date,'time':time,'date_time':date_time}
        self.dataset=pd.concat([self.data[['contact_number','contact_name','call_type','duration']],pd.DataFrame(date_time)],axis=1)
        self.dataset['date'] = pd.to_datetime(self.dataset['date'])
        self.dataset['date_time'] =pd.to_datetime(self.dataset['date_time'])
        callperday=self.dataset.groupby(self.dataset[self.dataset['call_type']!='MISSED']['date'].dt.strftime('%d'))['call_type']
        avgcall=callperday.count().sum()/31
        return avgcall


    #Average call duration per day
    def avgcalldurationperday(self):
        avgcallduration=self.dataset.groupby(self.dataset['date'].dt.strftime('%d'))['duration']
        return avgcallduration.sum().sum()/31

    #Average incoming calls per day
    def avgincomingcallperday(self):
        avgincomingcallperday=self.dataset.groupby(self.dataset[self.dataset['call_type']=='INCOMING']['date'].dt.strftime('%d'))['call_type']
        return avgincomingcallperday.count().sum()/31

    #Average incoming call duration per day
    def avgincomingcalldurationperday(self):
        avgincomingduration=self.dataset.groupby(self.dataset[self.dataset['call_type']=='INCOMING']['date'].dt.strftime('%d'))['duration']
        return avgincomingduration.sum().sum()/31

    #Average outgoing calls per day
    def avgoutgoingcallsperday(self):
        avgoutgoingcallperday=self.dataset.groupby(self.dataset[self.dataset['call_type']=='OUTGOING']['date'].dt.strftime('%d'))['call_type']
        return avgoutgoingcallperday.count().sum()/31

    #Average outgoing call duration per day
    def avgoutgoingcalldurationperday(self):
        avgoutgoingduration=self.dataset.groupby(self.dataset[self.dataset['call_type']=='OUTGOING']['date'].dt.strftime('%d'))['duration']
        return avgoutgoingduration.sum().sum()/31

    #No of unique individuals who called in
    def unique_individualscallsin(self):
        filter_data=self.dataset[self.dataset['call_type']=='INCOMING']['contact_name'].fillna('Unknown')
        return len(filter_data.unique())

    #No of unique individuals to whom called out
    def unique_individualscallsout(self):
        filter_data=self.dataset[self.dataset['call_type']=='OUTGOING']['contact_name'].fillna('Unknown')
        return len(filter_data.unique())

    #Hour of day with highest answer rate
    def hourofday_highestanswerrate(self):
        filter_dataset=self.dataset.groupby(self.dataset[self.dataset['call_type']!='MISSED']['date_time'].dt.strftime('%H'))['call_type']
        filter_data=dict(filter_dataset.count())
        max_key = max(filter_data, key=filter_data.get)
        return max_key

    #Hour of day with highest missed call rate
    def hourofday_highestmissedcallrate(self):
        filter_dataset=self.dataset.groupby(self.dataset[self.dataset['call_type']=='MISSED']['date_time'].dt.strftime('%H'))['call_type']
        filter_data=dict(filter_dataset.count())
        max_key = max(filter_data, key=filter_data.get)
        return max_key

    #most common person talk
    def mostcommonperson(self):
        filter_data_number=dict(self.dataset.groupby(self.dataset['contact_number'])['duration'].sum())
        max_dur_no=max(filter_data_number,key=filter_data_number.get)
        commonperson=self.dataset[self.dataset['contact_number']==max_dur_no]['contact_name'].iloc[0]
        return commonperson

    #most freq call person talk
    def mostfreqperson(self):
        number_list=self.dataset['contact_number'].tolist()
        most_freq=statistics.mode(number_list)
        data={'name':self.dataset[self.dataset['contact_number']==most_freq]['contact_name'].iloc[0],'counts':number_list.count(most_freq)}
        return data

    def json_output(self,dataframe):
        self.read_data(dataframe)
        call_answer_rate=self.callanswerrate()
        avg_call_perday=self.avgcallperday()
        avg_call_durationperday=self.avgcalldurationperday()
        avg_incomingcallperday=self.avgincomingcallperday()
        avg_incomingcalldurationperday=self.avgincomingcalldurationperday()
        avg_outgoingcallsperday=self.avgoutgoingcallsperday()
        avg_outgoingcalldurationperday=self.avgoutgoingcalldurationperday()
        unique_individuals_callsin=self.unique_individualscallsin()
        unique_individuals_callsout=self.unique_individualscallsout()
        hourofday_highest_answerrate=self.hourofday_highestanswerrate()
        hourofday_highest_missedcallrate=self.hourofday_highestmissedcallrate()
        most_commonperson=self.mostcommonperson()
        mostfreqperson=self.mostfreqperson()
        json_data={'call answer rate':call_answer_rate,
                   'avg call per day':avg_call_perday,
                   'avg call duration per day':avg_call_durationperday,
                   'avg incoming call per day':avg_incomingcallperday,
                   'avg incoming call duration per day':avg_incomingcalldurationperday,
                   'avg outgoing call per day':avg_outgoingcallsperday,
                   'avg outgoing call duration per day':avg_outgoingcalldurationperday,
                   'unique individuals calls in':unique_individuals_callsin,
                   'unique individuals calls out':unique_individuals_callsout,
                   'hour of day highest answer rate':hourofday_highest_answerrate,
                   'hour of day highest missedcall rate':hourofday_highest_missedcallrate,
                   'most common person':most_commonperson,
                   'most freq person':mostfreqperson,
                   }
        return json_data

    def new_format_report(self,call_type,ds):
        total_incoming_ds=ds[ds['call_type']==call_type]
        total_incoming_ds=total_incoming_ds.sort_values(by='date',ascending=False)
        groups=ds.groupby(total_incoming_ds['date'])

        data={}
        for name,group in groups:
            total_duration=float(group['duration'].sum())
            noofcalls=int(group['duration'].count())
            avg_call_duration=total_duration/noofcalls
            unique_individuals_calls=group['contact_number'].value_counts().to_dict()
            group['hour']=[t.split(":")[0] for t in group['time']]
            hourofday_highestanswerrate=group['hour'].max()
        #     print(avg_call_duration,hourofday_highestanswerrate)
        #     print(name,total_duration_sum,noofcalls,avg_call_duration,unique_individuals_calls)
            data[name]={'total duration':total_duration,'no of calls':noofcalls,"avg call duration":avg_call_duration,"unique individuals calls":unique_individuals_calls,"hour of day highest answer rate":hourofday_highestanswerrate}

        temp_data=pd.DataFrame(data)
        temp_data=temp_data.T
        # temp_data.index = pd.to_datetime(temp_data.index)
        avg_total_duration=temp_data['total duration'].sum()/len(temp_data['total duration'])
        avg_noofcalls=sum(temp_data['no of calls'])/len(temp_data['no of calls'])
        total_noofcalls=sum(temp_data['no of calls'])
        avg_call_duration=sum(temp_data['avg call duration'])/len(temp_data['avg call duration'])
        # print(type(avg_call_duration))
        # import pdb; pdb.set_trace()

        final_data={'avg total duration':avg_total_duration,'avg no of calls':avg_noofcalls,'avg call duration':avg_call_duration}


        temp_data=temp_data.sort_index(ascending=False).T.to_dict()
        data=dict(zip([k.strftime("%d/%m/%Y") for k,v in temp_data.items()],temp_data.values()))

        final_data['days wise reports']=data

        return final_data



    #data preprocessing
    def extract_features(self,call_logs,sms,num_contact):
        total_num_contacts_interacted_with=len(call_logs['contact_number'].unique())
        total_interactions=len(call_logs['contact_number'])
        total_duration=call_logs['duration'].sum()
    #     num_contacts=len(phone_book['contact_name'].unique())
        num_contacts=num_contact
        ave_message_body_length=len(" ".join([str(s) for s in sms['body_messages'].to_list()]))/len(sms['body_messages'])
        ave_daily_sms=sms['date'].count()/len(sms['date'].unique())
        ave_daily_contacts_interacted_with=total_num_contacts_interacted_with/len(call_logs['date'].unique())
        ave_daily_calls=len(call_logs)/len(call_logs['date'].unique())
        y=[total_num_contacts_interacted_with,total_interactions,total_duration,num_contacts,ave_message_body_length,ave_daily_sms,ave_daily_contacts_interacted_with,ave_daily_calls]
        return y

    def extractmonthlydata(self):
        call_logs=pd.read_csv("alternatedata_api/reports_module/dump_alternate_csv/call_logs.csv")
        # phone_book=pd.read_csv("alternatedata_report/reports_module/dump_alternate_csv/phone_book.csv")

        sms=pd.read_csv("alternatedata_api/reports_module/dump_alternate_csv/mail_and_messages.csv")

        #----------extract call log monthly group----------
        groups_date=[]
        for d in call_logs['date'].tolist():
            datedata=dt.datetime.strptime(d,'%a %b %d %H:%M:%S %Z%z %Y')
            groups_date.append(datedata.strftime("%m/%Y"))
        call_logs['date']=groups_date

        calllogs_groups=call_logs.groupby(call_logs['date'])

        #<-----------extract sms monthly group---------------
        sms['message_date']=pd.to_datetime(sms['message_date'])
        groups_date=[]
        for d in sms['message_date']:
            groups_date.append(d.strftime("%m/%Y"))
        sms['date']=pd.to_datetime(groups_date)
        sms_groups=sms.groupby(sms['date'].dt.strftime("%m/%Y"))

        data=[]
        month_name=[]
        for name1,g1 in calllogs_groups:
            for name2,g2 in sms_groups:
                if name1==name2:
                    data.append((g1,g2))
                    month_name.append(name1)
                    print(name1)
        #             print("----")
        return data,month_name

    def get_score(self):
        x=[]
        phone_book=pd.read_csv("alternatedata_api/reports_module/dump_alternate_csv/phone_book.csv")
        num_contacts=len(phone_book['contact_name'].unique())
        data,month_name=self.extractmonthlydata()
        for cl,sm in data:
            p=self.extract_features(cl,sm,num_contacts)
            x.append(p)
        #     break
        model=pickle.load(open("alternatedata_api/reports_module/save_objs/repayscoremodel.obj","rb"))

        score=model.predict_proba(x)
        month_scorelist=[a*100 for a in score[:,1].tolist()]
        json_report={"total score":sum(score[:,1])*100/len(score),"monthly_score":dict(zip(month_name,month_scorelist))}
        return json_report


    def json_reports(self,path):
        ds=self.read_data(path)
        date=[]
        time=[]
        date_time=[]
        for d in ds['date'].tolist():
            datedata=datetime.strptime(d,'%a %b %d %H:%M:%S %Z%z %Y')
            date.append(datedata.strftime("%d/%m/%Y"))
            time.append(datedata.strftime("%H:%M:%S"))
            date_time.append(datedata.strftime("%d/%m/%Y %H:%M:%S"))

        ds['date']=date
        ds['date_time']=date_time
        ds['time']=time
        ds['date']=pd.to_datetime(ds['date'])
        ds=ds.sort_values(by='date')

        ds=ds.reset_index()
        ds=ds.drop('index',axis=1)

        start_date=ds['date'].iloc[-1]

        end_date=ds['date'].iloc[0]
        data_duration=start_date-end_date
        data_duration=data_duration.days

        json_output={}
        json_output['data duration']={'start date':start_date.strftime("%d/%m/%Y"),'end date':end_date.strftime("%d/%m/%Y"),'days':data_duration}
        for call_type in ['INCOMING','OUTGOING','MISSED']:
            json_output[call_type]=self.new_format_report(call_type,ds)

        score=self.get_score()
        json_output['score']=score
        return json_output






    def missedcallreports(self,path):

        ds=self.read_data(path)
        print(path)

        #<---------------------data preprocessing-----------------------
        duration=[str(d//3600)+":"+str(d//60)+":"+str(d%60) for d in ds['duration']]

        time_zero = dt.datetime.strptime('00:00:00', '%H:%M:%S')

        date=[]
        time=[]
        date_time=[]
        for d in ds['date'].tolist():
            datedata=dt.datetime.strptime(d,'%a %b %d %H:%M:%S %Z%z %Y')
            # date.append(datedata.strftime("%d/%m/%Y"))
            date.append(datedata.strftime("%Y/%m/%d"))
            time.append(datedata.strftime("%H:%M:%S"))
            date_time.append(datedata.strftime("%d/%m/%Y %H:%M:%S"))

        end_call_time=[]
        for d in range(len(duration)):
            a=dt.datetime.strptime(time[d],"%H:%M:%S")
            b=dt.datetime.strptime(duration[d],"%H:%M:%S")
            time_zero=dt.datetime.strptime('00:00:00', '%H:%M:%S')
            end_call_time.append((a-time_zero+b).strftime("%H:%M:%S"))


        ds['date']=date
        ds['time']=time
        ds['end_call_time']=end_call_time

        groups=ds.groupby('date')

        #<----------------------------datetime calculation-----------------------
        json_data={}
        for name, group in groups:
            json_data[name]=[]
            missed_called=group[group['call_type']=='MISSED']
            outgoing_called=group[group['call_type']=='OUTGOING']
            incoming_called=group[group['call_type']=='INCOMING']
            nonmissed=group[group['call_type']!='MISSED']
            numbers=missed_called['contact_number'].value_counts()
            for n,counts in numbers.items():
                #<------------------------------------------
                contact_name=missed_called[missed_called['contact_number']==n]['contact_name'].tolist()[0]
                missed_logid=missed_called[missed_called['contact_number']==n]['call_log_id'].tolist()[-1]
                outgoing_logid=outgoing_called[outgoing_called['contact_number']==n]['call_log_id'].tolist()
                incoming_logid=incoming_called[incoming_called['contact_number']==n]['call_log_id'].tolist()
                #<---------------------------------------
                missed_times=missed_called[missed_called['contact_number']==n]['time'].to_list()
                inoutcall_timesnap=nonmissed[nonmissed['contact_number']!=n][['time','end_call_time']].values
                for t in missed_times:
                    t=dt.datetime.strptime(t,"%H:%M:%S")
                    for a,b in inoutcall_timesnap:
                        a=dt.datetime.strptime(a,"%H:%M:%S")
                        b=dt.datetime.strptime(b,"%H:%M:%S")
                        if a<t<b:
                            json_data[name].append({'number':n,'contact name':contact_name,'missedcall counts':counts,'status':"call missed due busy"})
                            # print(name,n,counts,contact_name,"missed call due busy")
                            # break

                for logid in outgoing_logid:
                    if missed_logid<logid:
                        json_data[name].append({'number':n,'contact name':contact_name,'missedcall counts':counts,'status':"call back"})
        #                 print(name,n,counts,contact_name,"call back")
                        break
                for logid in incoming_logid:
                    if missed_logid<logid:
                        json_data[name].append({'number':n,'contact name':contact_name,'missedcall counts':counts,'status':"received call after missed call"})
        #                 print(name,n,counts,contact_name,"received call after missed call")
                        break
                if len(outgoing_logid)==0 and len(incoming_logid)==0:
                    json_data[name].append({'number':n,'contact name':contact_name,'missedcall counts':counts,'status':"missed call"})
        #             print(name,n,counts,contact_name,"missed call")

        # dates=pd.DataFrame(json_data.keys())
        # dates[0]=pd.to_datetime(dates[0])
        # dates=dates.sort_values(0,ascending=False)
        # sorted_dates=dates[0].map(lambda x:x.strftime("%d/%m/%Y")).values
        # sorted_json={}
        # for a in sorted_dates:
        #     for b in json_data.keys():
        #         if a in b:
        #             sorted_json[b]=json_data[b]

        return json_data

#
# obj=calls_log_report()
# data=obj.json_output('call_logs.csv')
