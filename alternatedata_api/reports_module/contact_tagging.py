import pandas as pd
import numpy as np
import spacy
import nltk
from nltk.probability import FreqDist
# nlp=spacy.load("en_core_web_sm")

# custom_entity_input={'office':['maam','mam','sir']}


class phonebook_mapping:
    def read_data(self,path):
        ds=pd.read_csv(path)
        self.dataset=ds[['contact_name','contact_number']]

        return self.dataset

    #tagging with most freq keywords-----
    def tagging_most_freq_keywords(self):
        name_list=self.dataset['contact_name'].to_list()
        string=" ".join(name_list)
        string=string.lower()
        words = nltk.tokenize.word_tokenize(string)
        fdist = FreqDist(words)
        fdist=dict(fdist)
        tags=[k for k,v in fdist.items() if v>3 ]
        name_dict={}
        names=[]
        name_number_array=self.dataset[['contact_name','contact_number']].values
        for t in tags:
            for n in range(len(name_number_array)):
                if t in name_number_array[n][0].lower():
                    if t in name_dict.keys():
                      pass
                      name_dict[t][name_number_array[n][0]]=name_number_array[n][1]
                      # name_dict[t].append({name_number_array[n][0]:name_number_array[n][1]})
                    else:
                        name_dict[t]={name_number_array[n][0]:name_number_array[n][1]}
                    names.append(name_number_array[n][0])
        # names,dataset['contact_name'].tolist()
        self.excluding_contact=[(n,num) for n,num in self.dataset[['contact_name','contact_number']].values if n not in names]

        return name_dict,self.excluding_contact

    def tagging_with_nlp(self):
        #extract org and person tagging using nlp------
        org=[]
        person=[]
        extra1=[]
        nlp=spacy.load("en_core_web_sm")
        for e in range(len(self.excluding_contact)):
            doc=nlp(self.excluding_contact[e][0])
        #     print("------------------")
        #     for token in doc:
        #         print(token.text+"   "+token.pos_)

            for ent in doc.ents:
        #         print(ent.text+"    "+ent.label_+"----"+str(spacy.explain(ent.label_)))
                if ent.label_=='ORG':
                    org.append([self.excluding_contact[e][0],self.excluding_contact[e][1]])
                elif ent.label_ == 'PERSON':
                    person.append([self.excluding_contact[e][0],self.excluding_contact[e][1]])
                else:
                    extra1.append(self.excluding_contact[e])

        org=dict(org)
        person=dict(person)
        self.excluding_contact=dict(extra1)

        return org,person,self.excluding_contact

      #-------add custom entity----------------------
    def taggingwithcustomentity(self,custom_entity_input):
        custom_entity_output={}
        for key in custom_entity_input.keys():
            array_list=[]
            for value in custom_entity_input[key]:

                array=self.dataset[self.dataset['contact_name'].str.lower().str.contains(value,regex=True)].values
                array_list.append(array)
            custom_entity_output[key]=dict(np.concatenate(array_list))
        return custom_entity_output

    def getjsonoutput(self,dataframe,custom_entity_input):
        self.read_data(dataframe)
        data,extra=self.tagging_most_freq_keywords()
        org,person,extra=self.tagging_with_nlp()
        custom_entity=self.taggingwithcustomentity(custom_entity_input)
        data['organization entity']=org
        data['person entity']=person
        data.update(custom_entity)

        return data


# obj=phonebook_mapping()
# data=obj.getjsonoutput("phone_book.csv",custom_entity_input)
