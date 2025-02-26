from pandas import read_csv, Series, DataFrame
import pandas as pd

from rdflib import Graph, URIRef, Literal 
from rdflib.namespace import DC, OWL, FOAF, RDF, DCTERMS, Namespace, XSD
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore

import json 
import sqlite3
from sqlite3 import connect

from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get
from pprint import pprint

class IdentifiableEntity:
    def __init__(self, id:str):
        self.id= id
    def getId(self):
        return self.id
    
#APRIL

class Person(IdentifiableEntity):
    def __init__(self, id:str, name:str):
        super().__init__(id)
        self.name = name
    def getName(self):
        return self.name


class CulturalHeritageObject(IdentifiableEntity):
    def __init__(self, id: str, title: str, date: str, owner: str, place: str, authors):
        super().__init__(id)
        self.title = title
        self.date = date if date else None
        self.owner = owner
        self.place = place
        self.hasAuthor = []  
        self.desc= "Cultural Heritage Object"
        if type(authors) == list:  
            for author in authors:
                s_author= str(author)
                self.hasAuthor.append(s_author)
                if "(" in s_author:   
                     author_name, author_id = s_author.split("(") 
                     author_id =author_id.strip(")")
                     self.hasAuthor.append(Person(id=author_id, name=author_name))  
                else:
                     self.hasAuthor.append(Person(id="", name= s_author))
        elif type(authors) == str:
            authors_list= [] 
            for x in authors.split(";"):
                authors_list.append(x)
            for x in authors_list:
                if ("(") in x:
                     author_name, author_id = x.split("(") 
                     author_id =author_id.strip(")")
                     self.hasAuthor.append(Person(id=author_id, name=author_name))
                else:
                    self.hasAuthor.append(Person(id="", name= x))

            

    def getTitle(self):
        return self.title

    def getDate(self):
        return self.date

    def getOwner(self):
        return self.owner

    def getPlace(self):
        return self.place

    def getAuthors(self):
       if not self.hasAuthor:
             return []
       else:
          return self.hasAuthor
    
    def getType(self):
        return self.desc 

    
     
 
class NauticalChart(CulturalHeritageObject):
    def __init__ (self, id: str, title: str, date: str, owner: str, place: str, authors: list = None):
        super().__init__(id, title, date, owner, place, authors)
        self.desc= "nautical chart"
   
  
class Herbarium(CulturalHeritageObject):
    def __init__ (self, id: str, title: str, date: str, owner: str, place: str, authors: list = None):
        super().__init__(id, title, date, owner, place, authors)
        self.desc="herbarium"


class PrintedMaterial(CulturalHeritageObject):
    def __init__ (self, id:str, title:str, date:str, owner:str, place:str, authors:list = None):
        super().__init__(id, title, date, owner, place, authors)
        self.desc= "printed material"


class PrintedVolume(CulturalHeritageObject):
    def __init__ (self, id:str, title:str, date:str, owner:str, place:str, authors:list = None):
        super().__init__(id, title, date, owner, place, authors)
        self.desc= "printed volume"

    
class Painting(CulturalHeritageObject):
    def __init__ (self, id:str, title:str, date:str, owner:str, place:str, authors:list = None):
        super().__init__(id, title, date, owner, place, authors)
        self.desc= "painting"


class Map(CulturalHeritageObject):
    def __init__ (self, id:str, title:str, date:str, owner:str, place:str, authors:list = None):
        super().__init__(id, title, date, owner, place, authors)
        self.desc= "map"

class Specimen(CulturalHeritageObject):
    def __init__ (self, id:str, title:str, date:str, owner:str, place:str, authors:list = None):
        super().__init__(id, title, date, owner, place, authors)
        self.desc= "specimen"

    
class ManuscriptPlate(CulturalHeritageObject):
    def __init__ (self, id:str, title:str, date:str, owner:str, place:str, authors:list = None):
        super().__init__(id, title, date, owner, place, authors)
        self.desc= "manuscript plate"


    
class ManuscriptVolume(CulturalHeritageObject):
    def __init__ (self, id:str, title:str, date:str, owner:str, place:str, authors:list = None):
        super().__init__(id, title, date, owner, place, authors)
        self.desc= "manuscript volume"

   
    
class Model(CulturalHeritageObject):
    def __init__ (self, id:str, title:str, date:str, owner:str, place:str, authors:list = None):
        super().__init__(id, title, date, owner, place, authors)
        self.desc= "model"


#TERESA
class Activity:
    def __init__(self, institute:str, person:str, tool:str, start:str, end:str, refersTo:CulturalHeritageObject): 
        self.institute= institute
        self.person= person
        self.start= start
        self.end= end
        self.tools= set()
        if "," in tool:
             for t in tool.split(","): 
                  self.tools.add(t)
        else:
            self.tools.add(tool)
        self.ref= refersTo   
        self.desc="Activity"
    

    def getType(self):
        return self.desc  


    def getResponsibleInstitute(self):
        return self.institute 
    
    def getResponsiblePerson(self):
        if len(self.person) != 0:
            return self.person 
        else:
            return None
    
    def getTools(self):
        return self.tools
    
    def getStartDate(self):
        if len(self.start) != 0:
           return self.start 
        else:
            return None
    
    def getEndDate(self):
         if len(self.end) != 0:
           return self.end
         else:
            return None
    
    def refersTo(self):
        return self.ref

class Acquisition(Activity):
    def __init__(self, institute:str, person:str, tool:str, start:str, end:str, refersTo:CulturalHeritageObject, technique:str):
        super().__init__(institute, person, tool, start, end, refersTo)
        self.technique= technique
        self.desc="Acquisition"
    
    def getTechnique(self):
        return self.technique

class Processing(Activity):
    def __init__(self, institute:str, person:str, tool:str, start:str, end:str, refersTo:CulturalHeritageObject):
         super().__init__(institute, person, tool, start, end, refersTo)
         self.desc="Processing"
         
class Modelling (Activity):
    def __init__(self, institute:str, person:str, tool:str, start:str, end:str, refersTo:CulturalHeritageObject):
        super().__init__(institute, person, tool, start, end, refersTo)
        self.desc="Modelling"
         
class Optimising(Activity):
    def __init__(self, institute:str, person:str, tool:str, start:str, end:str, refersTo:CulturalHeritageObject):
        super().__init__(institute, person, tool, start, end, refersTo)
        self.desc="Optimising"
         
class Exporting(Activity):
    def __init__(self, institute:str, person:str, tool:str, start:str, end:str, refersTo:CulturalHeritageObject):
        super().__init__(institute, person, tool, start, end, refersTo)
        self.desc="Exporting"

#JIAYI
class Handler:
    def __init__(self):
        self.dbPathOrUrl = ""

    def getDbPathOrUrl(self):
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, newdb_path):
        try:
            self.dbPathOrUrl = newdb_path
            return True 
        except Exception as e:
            return False
       

from abc import ABC, abstractmethod

class UploadHandler(Handler, ABC):
    def __init__(self):
         super().__init__()

    @abstractmethod
    def pushDataToDb(self, file_path):
        pass


#JIAYI
class ProcessDataUploadHandler(UploadHandler): 
    def __init__(self):
        super().__init__()
        
    
    def pushDataToDb(self, file):
        try:
            with open(file, 'r') as f:
                 data = json.load(f)

            self.conn= sqlite3.connect(self.getDbPathOrUrl())
            self.cursor= self.conn.cursor()     
            self.cursor.execute('''
             CREATE TABLE IF NOT EXISTS objects (
                object_id TEXT PRIMARY KEY
             )
             ''')
            self.cursor.execute('''
             CREATE TABLE IF NOT EXISTS acquisition (
               object_id TEXT PRIMARY KEY,
               responsible_institute TEXT,
               responsible_person TEXT,
               technique TEXT,
               tool TEXT,
               start_date TEXT,
               end_date TEXT,
              FOREIGN KEY(object_id) REFERENCES objects(object_id)
             )
              ''')
            self.cursor.execute('''
             CREATE TABLE IF NOT EXISTS processing (
               object_id TEXT PRIMARY KEY,
               responsible_institute TEXT,
               responsible_person TEXT,
               tool TEXT,
               start_date TEXT,
               end_date TEXT,
               FOREIGN KEY(object_id) REFERENCES objects(object_id)
             )
             ''')
            self.cursor.execute('''
             CREATE TABLE IF NOT EXISTS modelling (
                object_id TEXT PRIMARY KEY,
                responsible_institute TEXT,
                responsible_person TEXT,
                tool TEXT,
                start_date TEXT,
                end_date TEXT,
                FOREIGN KEY(object_id) REFERENCES objects(object_id)
             )
             ''')

            self.cursor.execute('''
             CREATE TABLE IF NOT EXISTS optimising (
                object_id TEXT PRIMARY KEY,
                responsible_institute TEXT,
                responsible_person TEXT,
                tool TEXT,
                start_date TEXT,
                end_date TEXT,
                FOREIGN KEY(object_id) REFERENCES objects(object_id)
             )
             ''')

            self.cursor.execute('''
              CREATE TABLE IF NOT EXISTS exporting (
               object_id TEXT PRIMARY KEY,
                responsible_institute TEXT,
                responsible_person TEXT,
               tool TEXT,
               start_date TEXT,
               end_date TEXT,
               FOREIGN KEY(object_id) REFERENCES objects(object_id)
             )
             ''')
       



            for obj in data:
                object_id = obj['object id']
                self.cursor.execute("SELECT object_id FROM objects WHERE object_id = ?", (object_id,))
                result = self.cursor.fetchone()
                if result is None:
                     self.cursor.execute("INSERT INTO objects (object_id) VALUES (?)", (object_id,))
                else:
                     pass

                acquisition = obj['acquisition']
                self.cursor.execute("SELECT object_id FROM acquisition WHERE object_id = ?", (object_id,))
                result = self.cursor.fetchone()
                if result is None:
                    self.cursor.execute("INSERT INTO acquisition VALUES (?, ?, ?, ?, ?, ?, ?)", 
                             (object_id, acquisition['responsible institute'], acquisition['responsible person'], 
                             acquisition['technique'], ', '.join(acquisition['tool']), acquisition['start date'], acquisition['end date']))
                else:
                     pass
                 

                processing = obj['processing']
                self.cursor.execute("SELECT object_id FROM processing WHERE object_id = ?", (object_id,))
                result = self.cursor.fetchone()
                if result is None:
                    self.cursor.execute("INSERT INTO processing VALUES (?, ?, ?, ?, ?, ?)", 
                           (object_id, processing['responsible institute'], processing['responsible person'], 
                           ', '.join(processing['tool']), processing['start date'], processing['end date']))
                else:
                     pass
                

                modelling = obj['modelling']
                self.cursor.execute("SELECT object_id FROM modelling WHERE object_id = ?", (object_id,))
                result = self.cursor.fetchone()
                if result is None:
                     self.cursor.execute("INSERT INTO modelling VALUES (?, ?, ?, ?, ?, ?)", 
                           (object_id, modelling['responsible institute'], modelling['responsible person'], 
                           ', '.join(modelling['tool']), modelling['start date'], modelling['end date']))
                else:
                     pass
            
            
                optimising = obj['optimising']
                self.cursor.execute("SELECT object_id FROM optimising WHERE object_id = ?", (object_id,))
                result = self.cursor.fetchone()
                if result is None:
                        self.cursor.execute("INSERT INTO optimising VALUES (?, ?, ?, ?, ?, ?)", 
                           (object_id, optimising['responsible institute'], optimising['responsible person'], 
                            ', '.join(optimising['tool']), optimising['start date'], optimising['end date']))
                else:
                     pass
            
        
                exporting = obj['exporting']
                self.cursor.execute("SELECT object_id FROM exporting WHERE object_id = ?", (object_id,))
                result = self.cursor.fetchone()
                if result is None:
                        self.cursor.execute("INSERT INTO exporting VALUES (?, ?, ?, ?, ?, ?)", 
                           (object_id, exporting['responsible institute'], exporting['responsible person'], 
                           ', '.join(exporting['tool']), exporting['start date'], exporting['end date']))
                else:
                     pass
            
        
             
            self.conn.commit()
            return True
        except Exception as e:
             return False, 
        except sqlite3.OperationalError as e:
            return False
  
         

    def close(self):
        self.cursor.close()
        self.conn.close()

#TERESA

class MetadataUploadHandler(UploadHandler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self, file): 
        
       
        try:
             g= Graph()
             base_url= Namespace("https://example.com/ourproject/")
             g.bind("base_url", base_url)


             vbase="https://viaf.org/viaf/"
             ubase="https://www.getty.edu/vow/ULANFullDisplay?find=&role=&nation=&subjectid="

             place= URIRef ("https://www.wikidata.org/wiki/Property:P276")
             g.bind("place", place)
             tdic= dict( Nautical_chart= URIRef("https://www.wikidata.org/wiki/Q728502"),
                Herbarium= URIRef("https://www.wikidata.org/wiki/Q181916"),
                Printed_material= URIRef ("https://www.wikidata.org/wiki/Q1261026"),
                Printed_volume= URIRef("https://www.wikidata.org/wiki/Q1238720"),
                Painting= URIRef ("https://www.wikidata.org/wiki/Q11396303"),
                Map= URIRef ("https://www.wikidata.org/wiki/Q4006"),
                Specimen= URIRef("https://www.wikidata.org/wiki/Q85869058"),
                Manuscript_plate= URIRef("https://www.wikidata.org/wiki/Q188456"),
                Manuscript_volume= URIRef("https://www.wikidata.org/wiki/Q87167"), 
                Model= URIRef("https://www.wikidata.org/wiki/Q11784425"))
        

             base= read_csv(file, 
                   keep_default_na=False,
                    dtype= {
                          "Id": "string",
                          "Title": "string",
                          "Type": "string", 
                           "Date": "string",
                           "Author": "string",
                           "Owner": "string",
                           "Place": "string"
                      })

             obj_intid= {}
             for idx, row in base.iterrows(): 
                 local_id= "object" + str(idx)
                 subj= URIRef(str(base_url + local_id))

                 obj_intid[row["Id"]]= subj
        
                 g.add((subj, DC.title, Literal(row["Title"]))) 
                 if row["Date"] == "":
                     g.add((subj, DC.date, Literal("Not Defined")))
                 else:
                     g.add((subj, DC.date, Literal(row["Date"]))) 
                 g.add((subj, DC.identifier, Literal(row["Id"]))) 
                 g.add((subj, DCTERMS.rightsHolder, Literal(row["Owner"])))
                 g.add((subj, place, Literal(row["Place"])))


                 if ";" in row["Author"]:
                     aurow= row["Author"]
                     aus= aurow.split(";")
                     for author in aus:
                         g.add((subj, FOAF.maker, Literal(author)))
                 elif "Linnaeus" in row["Title"]:
                     pP= row["Title"].index("(") #position of par
                     pC= row["Title"].index(")") #position of comma
                     pA= row["Title"][(pP + 1): (pC)]
                     g.add((subj, FOAF.maker, Literal(pA)))
                 else:
                     g.add((subj, FOAF.maker, Literal(row["Author"])))
        

                 for p in g.objects(subj, FOAF.maker):
                     b= str(p)[0:6]
                     pu= URIRef(base_url + b)
                     g.add((pu, RDF.type, FOAF.Person))
                     if "VIAF" in str(p):
                         c= str(p)[(str(p).index("(")+6): (len(str(p))-1)]
                         cc= str(p)[(str(p).index("(")+1) :(len(str(p))-1)]
                         nu= URIRef(vbase + c + "/")
                         g.add((pu, DC.identifier, Literal(cc)))
                         g.add((pu, OWL.sameAs, nu))
                         g.add((pu, FOAF.made, subj))
                         g.add((pu, FOAF.name, Literal(p)))  
                     elif "ULAN" in str(p):
                         c= str(p)[(str(p).index("(")+6): (len(str(p))-1)]
                         cc= str(p)[(str(p).index("(")+1) :(len(str(p))-1)] 
                         nu=URIRef(ubase + c)
                         g.add((pu, DC.identifier, Literal(cc)))
                         g.add((pu, OWL.sameAs, nu))
                         g.add((pu, FOAF.made, subj))
                         g.add((pu, FOAF.name, Literal(p))) 
                     else:
                         g.add((pu, FOAF.name, Literal(p)))
                      
        
                 for x in tdic.keys():
                    if x== row["Type"] or x.replace("_", " ") in row["Type"]:
                          g.add((subj, RDF.type, Literal((x.replace("_", " ")))))
                    else:
                         None



             st= SPARQLUpdateStore()
             endpoint=self.getDbPathOrUrl()
             st.open((endpoint, endpoint))
             for triple in g.triples((None, None, None)):
                  st.add(triple)
             st.close()

             return True 
             
        except Exception as e: 
            return False, f"Error: not possible to create the database. Details: {str(e)}. Please, check the paths."

#APRIL

class QueryHandler(Handler):
    def __init__(self):
        super().__init__()

    def executeQuery(self, query: str):
        endpoint = self.getDbPathOrUrl()
        df = get(endpoint, query, True)
        return df.fillna("") if not df.empty else pd.DataFrame()

    @abstractmethod
    def getById(self, entity_id: str): #All
        pass
        
class ProcessDataQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()

    def getById(self, entity_id:str): #All
     with connect(self.getDbPathOrUrl()) as con:
            query_template = 'SELECT *, "{table}" as activity_type FROM {table} WHERE "object_id" LIKE ?;'
            tables = ["Acquisition", "Processing", "Modelling", "Optimising", "Exporting"]
            dataframes = [
                pd.read_sql(query_template.format(table=table), con, params=(f"%{entity_id}%",))
                for table in tables
            ]
            combined_df = pd.concat(dataframes, ignore_index=True)
            return combined_df.fillna("")

    def getAllActivities(self):      
        with connect(self.getDbPathOrUrl()) as con:      
            queries = [
                "SELECT *, 'Acquisition' as activity_type FROM Acquisition",
                "SELECT *, 'Processing' as activity_type FROM Processing",
                "SELECT *, 'Modelling' as activity_type FROM Modelling",
                "SELECT *, 'Optimising' as activity_type FROM Optimising",
                "SELECT *, 'Exporting' as activity_type FROM Exporting"
            ]
            dataframes = [pd.read_sql(query, con) for query in queries]
            combined_df = pd.concat(dataframes, ignore_index=True)
            return combined_df.fillna("")

    def getActivitiesByResponsibleInstitution(self, Insname:str):
        with connect(self.getDbPathOrUrl()) as con:
            query_template = 'SELECT *, "{table}" as activity_type FROM {table} WHERE "responsible_institute" LIKE ?;'
            tables = ["Acquisition", "Processing", "Modelling", "Optimising", "Exporting"]
            dataframes = [
                pd.read_sql(query_template.format(table=table), con, params=(f"%{Insname}%",))
                for table in tables
            ]
            combined_df = pd.concat(dataframes, ignore_index=True)
            return combined_df.fillna("")

    def getActivitiesByResponsiblePerson(self, Personname:str):
        with connect(self.getDbPathOrUrl()) as con:
            query_template = 'SELECT *, "{table}" as activity_type FROM {table} WHERE "responsible_person" LIKE ?;'
            tables = ["Acquisition", "Processing", "Modelling", "Optimising", "Exporting"]
            dataframes = [
                pd.read_sql(query_template.format(table=table), con, params=(f"%{Personname}%",))
                for table in tables
            ]
            combined_df = pd.concat(dataframes, ignore_index=True)
            return combined_df.fillna("")

    def getAcquisitionsByTechnique(self, Techname:str):
        with connect(self.getDbPathOrUrl()) as con:
            query_template = 'SELECT *, "{table}" as activity_type FROM {table} WHERE "technique" LIKE ?;'
            tables = ["Acquisition", "Processing", "Modelling", "Optimising", "Exporting"]
            dataframes = [
                pd.read_sql(query_template.format(table=table), con, params=(f"%{Techname}%",))
                for table in tables
            ]
            combined_df = pd.concat(dataframes, ignore_index=True)
            return combined_df.fillna("")

    def getActivitiesStartedAfter(self, date: str):
        with connect(self.getDbPathOrUrl()) as con:
            query_template = 'SELECT *, "{table}" as activity_type FROM {table} WHERE "start_date" >= ?;'
            tables = ["Acquisition", "Processing", "Modelling", "Optimising", "Exporting"]
            dataframes = [
                pd.read_sql(query_template.format(table=table), con, params=(date,))
                for table in tables
            ]
            combined_df = pd.concat(dataframes, ignore_index=True)
            return combined_df.fillna("")

    def getActivitiesEndedBefore(self, date: str):
        with connect(self.getDbPathOrUrl()) as con:
            query_template = 'SELECT *, "{table}" as activity_type FROM {table} WHERE "end_date" <= ? ;' 
            tables = ["Acquisition", "Processing", "Modelling", "Optimising", "Exporting"]              
            dataframes = [
                pd.read_sql(query_template.format(table=table), con, params=(date,))
                for table in tables
            ]
            combined_df = pd.concat(dataframes, ignore_index=True)
            return combined_df

    def getActivitiesUsingTool(self, Toolname: str):
        with connect(self.getDbPathOrUrl()) as con:
            query_template = 'SELECT *, "{table}" as activity_type FROM {table} WHERE "tool" LIKE ?;'
            tables = ["Acquisition", "Processing", "Modelling", "Optimising", "Exporting"]
            dataframes = [
                pd.read_sql(query_template.format(table=table), con, params=(f"%{Toolname}%",))
                for table in tables
            ]
            combined_df = pd.concat(dataframes, ignore_index=True)
            return combined_df.fillna("")
        
class MetadataQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()

    def getById(self, entity_id:str): #All
        object_query = f"""
        PREFIX dc:  <http://purl.org/dc/elements/1.1/>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/> 
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX dcterms: <http://purl.org/dc/terms/>

        SELECT DISTINCT ?id ?type ?title ?date ?owner ?place ?Authors
        WHERE {{
            ?obj dc:identifier '{entity_id}' .
            ?obj dc:identifier ?id .
            ?obj rdf:type ?type .
            ?obj dc:title ?title .
            ?obj <https://www.wikidata.org/wiki/Property:P276> ?place .
            ?obj dcterms:rightsHolder ?owner . 
            OPTIONAL {{ ?obj dc:date ?date . }}
            OPTIONAL {{
                ?obj foaf:maker ?Authors .
                ?author foaf:name ?Authors .
            }}
        }}
        """

        person_query = f"""
        PREFIX dc: <http://purl.org/dc/elements/1.1/>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/> 

        SELECT ?authorId ?authorName
        WHERE {{
            ?id dc:identifier '{entity_id}' .
            ?id dc:identifier ?authorId .
            ?id foaf:name ?authorName . 
        }}
        """

        df_object = self.executeQuery(object_query)
        df_person = self.executeQuery(person_query)

        return df_object if not df_object.empty else df_person 
    

    def getAllPeople(self):
        query = """
        PREFIX dc: <http://purl.org/dc/elements/1.1/>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/> 

        SELECT DISTINCT ?authorId ?authorName
        WHERE {
            ?person dc:identifier ?authorId .
            ?person foaf:name ?authorName .
        }
        """
        return self.executeQuery(query)

    def getAllCulturalHeritageObjects(self):
        
        query = f"""
        PREFIX dc: <http://purl.org/dc/elements/1.1/>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/> 
        PREFIX dcterms: <http://purl.org/dc/terms/>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 

        SELECT DISTINCT ?id ?type ?title ?date ?owner ?place ?Authors
        WHERE {{
            ?obj dc:identifier ?id .
            ?obj rdf:type ?type .
            ?obj dc:title ?title .
            ?obj dcterms:rightsHolder ?owner . 
            ?obj <https://www.wikidata.org/wiki/Property:P276> ?place .
            ?author foaf:name ?Authors .
            ?obj  foaf:maker ?Authors .
            OPTIONAL {{ ?obj dc:date ?date . }}
        }}
        """
        return self.executeQuery(query)

    def getAuthorsOfCulturalHeritageObject(self, object_id: str):
        query = f"""
        PREFIX dc: <http://purl.org/dc/elements/1.1/>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/> 

        SELECT ?authorId ?authorName
        WHERE {{
            ?object dc:identifier '{object_id}' .
            ?object foaf:maker ?authorName .
            ?author dc:identifier ?authorId .
            ?author foaf:name ?authorName .
        }}
        """
        return self.executeQuery(query)

    def getCulturalHeritageObjectsAuthoredBy(self, author_id: str):
        query = f"""
        PREFIX dc: <http://purl.org/dc/elements/1.1/>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/> 
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
        PREFIX dcterms: <http://purl.org/dc/terms/>

        SELECT ?type ?id ?title ?date ?owner ?place ?Authors
        WHERE {{
            ?author dc:identifier '{author_id}' .
            ?author foaf:name ?Authors .
            ?object foaf:maker ?Authors .
            ?object rdf:type ?type .
            ?object dc:title ?title .
            ?object <https://www.wikidata.org/wiki/Property:P276> ?place . 
            ?object dcterms:rightsHolder ?owner .
            ?object dc:identifier ?id .
            OPTIONAL {{ ?object dc:date ?date . }}
        }}
        """
        return self.executeQuery(query) 
    
#JIAYI

class BasicMashup(object):
    def __init__(self):
        self.metadataQuery = []  
        self.processQuery = [] 

    def cleanMetadataHandlers(self):
        self.metadataQuery = [] 
        return True
    
    def cleanProcessHandlers(self):
        self.processQuery = [] 
        return True
    
    def addMetadataHandler(self, metadataHandler): 
        self.metadataQuery.append(metadataHandler)   
        return True
        
    
    def addProcessHandler(self, processHandler):
        self.processQuery.append(processHandler)
        return True 


        


    def getEntityById(self, id: str):
        handler_list = self.metadataQuery
        df_list = []
        for handler in handler_list:
            entity = handler.getById(id)
            entity_update = pd.DataFrame(entity)
            df_list.append(entity_update)
            df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")
    
        for _, row in df_union.iterrows():
            if "authorName" in row:
                author = row['authorName']
                if author != "":
                    return Person(id=str(id), name=row['authorName'])
                else:
                    return None
            else:
                obj_type = row["type"]
                class_map = {
                    "Nautical chart": NauticalChart,
                    "Manuscript plate": ManuscriptPlate,
                    "Manuscript volume": ManuscriptVolume,
                    "Printed volume": PrintedVolume,
                    "Printed material": PrintedMaterial,
                    "Herbarium": Herbarium,
                    "Specimen": Specimen,
                    "Painting": Painting,
                    "Model": Model,
                    "Map": Map
                }
                for key, cls in class_map.items():
                    if key in obj_type:
                        return cls(id=str(id), title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])
                return None
            
    def getAllPeople(self): 
        df_list = [handler.getAllPeople() for handler in self.metadataQuery]
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")
    
        return [Person(id=str(row["authorId"]), name=row['authorName']) for _, row in df_union.iterrows()]
    

    def getAllCulturalHeritageObjects(self):        
        handler_list = self.metadataQuery
        df_list = []
        result = []
    
        for handler in handler_list:
            df_objects = handler.getAllCulturalHeritageObjects()
            df_list.append(df_objects)  
    
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")
        class_map = {
            "Nautical chart": NauticalChart,
            "Manuscript plate": ManuscriptPlate,
            "Manuscript volume": ManuscriptVolume,
            "Printed volume": PrintedVolume, 
            "Printed material": PrintedMaterial,
            "Herbarium": Herbarium,
            "Specimen": Specimen,
            "Painting": Painting,
            "Model": Model,
            "Map": Map
        }
    
        for _, row in df_union.iterrows():
            obj_type = row['type']
            for key, cls in class_map.items():
                if key in obj_type:
                    obj = cls(
                        id=str(row["id"]),
                        title=row['title'],
                        date=str(row['date']),
                        owner=row['owner'],
                        place=row['place'],
                        authors=row['Authors']
                    )
                    result.append(obj)
                    break
    
        return result


    def getAuthorsOfCulturalHeritageObject(self, id: str):
        result = []
        df_list = [handler.getAuthorsOfCulturalHeritageObject(id) for handler in self.metadataQuery]
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")
        for _, row in df_union.iterrows():
            author = row['authorName']
            if author:
                person = Person(id=str(row["authorId"]), name=row['authorName'])
                result.append(person)
        
        return result   
    

    #TERESA

    def getCO(self, df): 
        COl=[]
        for idx, row in df.iterrows():
                    p= df.iloc[idx]
                    f= p["type"]
                    sf=str(f)
                    if sf == "map" or sf == "Map":
                         o= Map(p["id"], p["title"], p["date"], p["owner"], p["place"], p["Authors"])
                         COl.append(o) 
                         
                    elif sf == "nautical chart" or sf== "Nautical chart":
                         o=NauticalChart(p["id"], p["title"], p["date"], p["owner"], p["place"], p["Authors"])
                         COl.append(o) 
                         
                    elif sf == "model" or sf== "Model":
                         o=Model(p["id"], p["title"], p["date"], p["owner"], p["place"], p["Authors"])
                         COl.append(o) 
                          
                    elif sf== "manuscript plate" or sf== "Manuscript place":
                         o=ManuscriptPlate(p["id"], p["title"], p["date"], p["owner"], p["place"], p["Authors"])
                         COl.append(o) 
                         
                    elif sf == "manuscript volume" or sf== "Manuscript volume":
                         o=ManuscriptVolume(p["id"], p["title"], p["date"], p["owner"], p["place"], p["Authors"])
                         COl.append(o) 
                              
                    elif sf== "printed material" or sf== "Printed material":
                         o=PrintedMaterial(p["id"], p["title"], p["date"], p["owner"], p["place"], p["Authors"])
                         COl.append(o) 
                          
                    elif sf== "printed volume" or sf== "Printed volume":
                         o=PrintedVolume(p["id"], p["title"], p["date"], p["owner"], p["place"], p["Authors"])
                         COl.append(o) 
                         
                    elif sf== "painting" or sf== "Painting":
                         o=Painting(p["id"], p["title"], p["date"], p["owner"], p["place"], p["Authors"])
                         COl.append(o) 
                          
                    elif sf== "specimen" or sf=="Specimen":
                         o=Specimen(p["id"], p["title"], p["date"], p["owner"], p["place"], p["Authors"])
                         COl.append(o) 

                          
                    elif sf== "herbarium" or sf=="herbarium":
                         o=Herbarium(p["id"], p["title"], p["date"], p["owner"], p["place"], p["Authors"])
                         COl.append(o)
            
        return COl
    
   
    



    def getCulturalHeritageObjectsAuthoredBy(self, id):
           l=[]
           if len(self.metadataQuery) != 0:
               for handler in self.metadataQuery:
                     df=handler.getCulturalHeritageObjectsAuthoredBy(id)
                     df.fillna("", inplace=True)
                     df.drop_duplicates()
                     
               for idx, row in df.iterrows():
                         p= df.iloc[idx]
                         f= p["type"]
                         sf=str(f)
                         if sf == "map" or sf == "Map":
                             o=Map(p["id"], p["title"], p["date"], p["owner"], p["place"], [p["Authors"]])
                             l.append(o)
                         elif sf == "nautical chart" or sf== "Nautical chart":
                             o=NauticalChart(p["id"], p["title"], p["date"], p["owner"], p["place"], [p["Authors"]])
                             l.append(o)
                         elif sf == "model" or sf== "Model":
                             o=Model(p["id"], p["title"], p["date"], p["owner"], p["place"], [p["Authors"]])
                             l.append(o)  
                         elif sf== "manuscript plate" or sf== "Manuscript place":
                             o=ManuscriptPlate(p["id"], p["title"], p["date"], p["owner"], p["place"], [p["Authors"]])
                             l.append(o)
                         elif sf == "manuscript volume" or sf== "Manuscript volume":
                             o=ManuscriptVolume(p["id"], p["title"], p["date"], p["owner"], p["place"], [p["Authors"]])
                             l.append(o)     
                         elif sf== "printed material" or sf== "Printed material":
                             o=PrintedMaterial(p["id"], p["title"], p["date"], p["owner"], p["place"], [p["Authors"]])
                             l.append(o) 
                         elif sf== "printed volume" or sf== "Printed volume":
                             o=PrintedVolume(p["id"], p["title"], p["date"], p["owner"], p["place"], [p["Authors"]])
                             l.append(o) 
                         elif sf== "painting" or sf== "Painting":
                             o=Painting(p["id"], p["title"], p["date"], p["owner"], p["place"], [p["Authors"]])
                             l.append(o) 
                         elif sf== "specimen" or sf=="Specimen":
                             o=Specimen(p["id"], p["title"], p["date"], p["owner"], p["place"], [p["Authors"]])
                             l.append(o) 
                         elif sf== "herbarium" or sf=="herbarium":
                             o=Herbarium(p["id"], p["title"], p["date"], p["owner"], p["place"], [p["Authors"]])
                             l.append(o) 
                         
               return l
                     
                
                       

 
    def getAllActivities(self):
         l=[]
         if len(self.metadataQuery) != 0:
             for handler in self.metadataQuery:
                 d=dict()
                 dfo= handler.getAllCulturalHeritageObjects()
                 dfo.fillna("", inplace=True)
                 dfo.drop_duplicates()
                 dfCO= BasicMashup()
                 dfCCO= dfCO.getCO(dfo)
                 for obj in dfCCO:
                     id= obj.getId()
                     d[str(id)]=obj
                
         if len(self.processQuery) != 0:
             for handler in self.processQuery:
                 df= handler.getAllActivities()
                 df.fillna("", inplace=True)
                 df.drop_duplicates()
                 for idx, row in df.iterrows():
                     obId= str(row["object_id"])
                  
                     x= row["activity_type"] 
                     if x== "Modelling" or x== "modelling": 
                         if obId in d.keys():
                                 co= d.get(obId)
                         o= Modelling(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co)
                         l.append(o)
                     elif x=="Optimising" or x=="optimising":
                          if obId in d.keys():
                                 co= d.get(obId)
                          o= Optimising(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co)
                          l.append(o)
                     elif x=="exporting" or x=="Exporting": 
                         if obId in d.keys():
                                 co= d.get(obId)
                         o= Exporting(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co)
                         l.append(o)
                     elif x=="acquisition" or x=="Acquisition":
                         if obId in d.keys():
                                 co= d.get(obId)
                         o= Acquisition(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co, row["technique"])
                         l.append(o)
                     elif x=="processing" or x=="Processing":
                          if obId in d.keys():
                                 co= d.get(obId)
                          o= Processing(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co)
                          l.append(o)
                
                     
                 
             return l


    
    
    
    
    def getActivitiesByResponsibleInstitution(self, name):
        l=[]
        d=dict()
        if len(self.metadataQuery) != 0:
             for handler in self.metadataQuery:
                 d=dict()
                 dfo= handler.getAllCulturalHeritageObjects()
                 dfo.fillna("", inplace=True)
                 dfo.drop_duplicates()
                 dfCO= BasicMashup()
                 dfCCO= dfCO.getCO(dfo)
                 for obj in dfCCO:
                     id= obj.getId()
                     d[str(id)]=obj
                
        if len(self.processQuery) != 0:
             for handler in self.processQuery:
                 df= handler.getActivitiesByResponsibleInstitution(name)
                 df.fillna("", inplace=True)
                 df.drop_duplicates()
                 for idx, row in df.iterrows():
                     obId= str(row["object_id"])
                 

                     inst= row["responsible_institute"]
                     x=row["activity_type"]
                     if inst== name or name in inst:
                         if x== "Modelling" or x== "modelling": 
                             if obId in d.keys():
                                 co= d.get(obId)
                             o= Modelling(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co)
                         elif x=="Optimising" or x=="optimising":
                             if obId in d.keys():
                                 co= d.get(obId)
                             o= Optimising(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co)
                         elif x=="exporting" or x=="Exporting": 
                             if obId in d.keys():
                                 co= d.get(obId)
                             o= Exporting(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co)
                         elif x=="acquisition" or x=="Acquisition":
                             if obId in d.keys():
                                 co= d.get(obId)
                             o= Acquisition(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co, row["technique"])
                         elif x=="processing" or x=="Processing":
                              if obId in d.keys():
                                 co= d.get(obId)
                              o= Processing(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co)
                
                     l.append(o) 

                 return l
             
                         

    

    def getActivitiesByResponsiblePerson(self, name):
        l=[]
        if len(self.metadataQuery) != 0:
             for handler in self.metadataQuery:
                 d=dict()
                 dfo= handler.getAllCulturalHeritageObjects()
                 dfo.fillna("", inplace=True)
                 dfo.drop_duplicates()
                 dfCO= BasicMashup()
                 dfCCO= dfCO.getCO(dfo)
                 for obj in dfCCO:
                     id= obj.getId()
                     d[str(id)]=obj
                
        if len(self.processQuery) != 0:
             for handler in self.processQuery:
                 df= handler.getActivitiesByResponsiblePerson(name)
                 df.fillna("", inplace=True)
                 df.drop_duplicates()
                 for idx, row in df.iterrows():
                     obId= str(row["object_id"])
                   
                     pers= row["responsible_person"]         
                     x=row["activity_type"]
                     if pers == name or name in pers:
                         if x== "Modelling" or x== "modelling": 
                             if obId in d.keys():
                                 co= d.get(obId)
                             o= Modelling(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co)
                         elif x=="Optimising" or x=="optimising":
                             if obId in d.keys():
                                 co= d.get(obId)
                             o= Optimising(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co)
                         elif x=="exporting" or x=="Exporting": 
                             if obId in d.keys():
                                 co= d.get(obId)
                             o= Exporting(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co)
                         elif x=="acquisition" or x=="Acquisition":
                             if obId in d.keys():
                                 co= d.get(obId)
                             o= Acquisition(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co, row["technique"])
                         elif x=="processing" or x=="Processing":
                              if obId in d.keys():
                                 co= d.get(obId)
                              o= Processing(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co)
                
                     l.append(o) 

                 return l
             else:
                 None
        
        
        
    def getActivitiesUsingTool(self,tool):
        l=[]
        if len(self.metadataQuery) != 0:
             for handler in self.metadataQuery:
                 d=dict()
                 dfo= handler.getAllCulturalHeritageObjects()
                 dfo.fillna("", inplace=True)
                 dfo.drop_duplicates()
                 dfCO= BasicMashup()
                 dfCCO= dfCO.getCO(dfo)
                 for obj in dfCCO:
                     id= obj.getId()
                     d[str(id)]=obj
                
        if len(self.processQuery) != 0:
             for handler in self.processQuery:
                 df= handler.getActivitiesUsingTool(tool)
                 df.fillna("", inplace=True)
                 df.drop_duplicates()
                 for idx, row in df.iterrows():
                     obId= str(row["object_id"])
                     to= row["tool"]         
                     x=row["activity_type"]
                     if to == tool or tool in to:
                         if x== "Modelling" or x== "modelling": 
                             if obId in d.keys():
                                 co= d.get(obId)
                             o= Modelling(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co)
                         elif x=="Optimising" or x=="optimising":
                             if obId in d.keys():
                                 co= d.get(obId)
                             o= Optimising(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co)
                         elif x=="exporting" or x=="Exporting": 
                             if obId in d.keys():
                                 co= d.get(obId)
                             o= Exporting(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co)
                         elif x=="acquisition" or x=="Acquisition":
                             if obId in d.keys():
                                 co= d.get(obId)
                             o= Acquisition(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co, row["technique"])
                         elif x=="processing" or x=="Processing":
                              if obId in d.keys():
                                 co= d.get(obId)
                              o= Processing(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co)
                
                     l.append(o) 

                 return l
             else:
                 None

    def getActivitiesStartedAfter(self,date):
        l=[]
        if len(self.metadataQuery) != 0:
             for handler in self.metadataQuery:
                 d=dict()
                 dfo= handler.getAllCulturalHeritageObjects()
                 dfo.fillna("N/A", inplace=True)
                 dfo.drop_duplicates()
                 dfCO= BasicMashup()
                 dfCCO= dfCO.getCO(dfo)
                 for obj in dfCCO:
                     id= obj.getId()
                     d[str(id)]=obj
                     
        
                
        if len(self.processQuery) != 0:
             for handler in self.processQuery:
                 df= handler.getActivitiesStartedAfter(date)
                 df.fillna("N/A", inplace=True)
                 df.drop_duplicates()
                 for idx, row in df.iterrows():
                     obId= row["object_id"]

                     dt= row["start_date"]         
                     x=row["activity_type"]

                     if x== "Modelling" or x== "modelling": 
                             if obId in d.keys():
                                 co= d.get(obId)
                             o= Modelling(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co)
                     elif x=="Optimising" or x=="optimising":
                             if obId in d.keys():
                                 co= d.get(obId)
                             o= Optimising(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co)
                     elif x=="exporting" or x=="Exporting": 
                             if obId in d.keys():
                                 co= d.get(obId)
                             o= Exporting(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co)
                     elif x=="acquisition" or x=="Acquisition":
                             if obId in d.keys():
                                 co= d.get(obId)
                             o= Acquisition(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co, row["technique"])
                     elif x=="processing" or x=="Processing":
                              if obId in d.keys():
                                 co= d.get(obId)
                              o= Processing(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co)
                
                     l.append(o) 

             return l
        else:
                None
       


    def getActivitiesEndedBefore(self, date):
        l=[]
        if len(self.metadataQuery) != 0:
             for handler in self.metadataQuery:
                 d=dict()
                 dfo= handler.getAllCulturalHeritageObjects()
                 dfo.fillna("N/A", inplace=True)
                 dfo.drop_duplicates()
                 dfCO= BasicMashup()
                 dfCCO= dfCO.getCO(dfo)
                 for obj in dfCCO:
                     id= obj.getId()
                     d[str(id)]=obj
                
                 
                
        if len(self.processQuery) != 0:
             for handler in self.processQuery:
                 df= handler.getActivitiesEndedBefore(date)
                 df.fillna("N/A", inplace=True)
                 df.drop_duplicates()
                 for idx, row in df.iterrows():
                     obId= str(row["object_id"])         
                     x=row["activity_type"]
                     
                     if x== "Modelling" or x== "modelling": 
                             if obId in d.keys():
                                 co= d.get(obId)
                             o= Modelling(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co)
                             
                     elif x=="Optimising" or x=="optimising":
                             if obId in d.keys():
                                 co= d.get(obId)
                             o= Optimising(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co)
                           
                     elif x=="exporting" or x=="Exporting": 
                             if obId in d.keys():
                                 co= d.get(obId)
                             o= Exporting(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co)
                            
                     elif x=="acquisition" or x=="Acquisition":
                             if obId in d.keys():
                                 co= d.get(obId)
                             o= Acquisition(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co, row["technique"])
                            
                     elif x=="processing" or x=="Processing":
                              if obId in d.keys():
                                 co= d.get(obId)
                              o= Processing(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co)
                              
                     l.append(o)     

        return l
             


    def getAcquisitionsByTechnique(self, technique):
       l=[]
       d=dict()
       if len(self.metadataQuery) != 0:
             for handler in self.metadataQuery:
                 dfo= handler.getAllCulturalHeritageObjects()
                 dfo.fillna("", inplace=True)
                 dfo.drop_duplicates()
                 dfCO= BasicMashup()
                 dfCCO= dfCO.getCO(dfo)
                 for obj in dfCCO:
                     id= obj.getId()
                     d[str(id)]=obj
                
       if len(self.processQuery) != 0:
             for handler in self.processQuery:
                 df= handler.getAcquisitionsByTechnique(technique)
                 df.fillna("", inplace=True)
                 df.drop_duplicates()
                 
                 for idx, row in df.iterrows():
                     o=""
                     obId= str(row["object_id"])
                     x= row["technique"]
                     if x== technique or technique in x:
                            if obId in d.keys():
                                 co= d.get(obId)
                            o= Acquisition(row["responsible_institute"], row["responsible_person"], row["tool"], row["start_date"], row["end_date"], co, row["technique"])
                            l.append(o) 

                 return l




#APRIL
class AdvancedMashup(BasicMashup):
    def __init__(self):
        super().__init__()
 
    def getActivitiesOnObjectsAuthoredBy(self, personId: str) -> list[Activity]:
        cultural_objects= self.getCulturalHeritageObjectsAuthoredBy(personId)
        if not cultural_objects:
            return []
        
        id_list= [obj.getId() for obj in cultural_objects]
        
        activities= self.getAllActivities()

        result_list=[]
        
        for activity in activities:
            if (activity.refersTo()).getId() in id_list:
                result_list.append(activity)

        return result_list
    


    def getObjectsHandledByResponsiblePerson(self, partialName: str) -> list[CulturalHeritageObject]:
        objects = []

        if self.processQuery:
                activities = self.getActivitiesByResponsiblePerson(partialName)
        

        if self.metadataQuery:
            object_list = self.getAllCulturalHeritageObjects()
            object_ids = {(activity.refersTo()).getId() for activity in activities}
            objects = [obj for obj in object_list if obj.getId() in str(object_ids)]

        return objects


    def getObjectsHandledByResponsibleInstitution(self, partialName: str) -> list[CulturalHeritageObject]:
          objects = []

   
          if self.processQuery:
              activities = self.getActivitiesByResponsibleInstitution(partialName)
             
        

          if self.metadataQuery:
                object_list = self.getAllCulturalHeritageObjects()
                object_ids = {(activity.refersTo()).getId() for activity in activities} 
                objects = [obj for obj in object_list if obj.getId() in str(object_ids)]

          return objects


    def getAuthorsOfObjectsAcquiredInTimeFrame(self, start_date: str, end_date: str) -> list[Person]:
        authors = []
        object_list=[]

        object_list = self.getAllCulturalHeritageObjects()
        activityl=[]
     

        for activity in self.getActivitiesEndedBefore(end_date):
            if isinstance(activity, Acquisition):
                if activity.getStartDate() == start_date or activity.getStartDate() >= start_date:
                    activityl.append(activity)
        

        for obj in object_list:
            for act in activityl:
                 x=act.refersTo()
                 if type(x) != str:
                     if str(x.getId())== str(obj.getId()):
                         authors_list = obj.getAuthors()
                         authors.extend(authors_list)
    
        
        return authors


#TERESA

def getLabelList(list): 
        l=[]
        e="Error: empty list"
        for x in list:
            if type(x) == Person:
                c_o= x.getName()
                e="These are the people's names"
                l.append(c_o)
            elif type(x)== CulturalHeritageObject or type(x) in CulturalHeritageObject.__subclasses__():
                c_o= x.getTitle()
                e="These are the objects' titles"
                l.append(c_o)
            elif type(x)== Activity or type(x) in Activity.__subclasses__():
                c_o= x.getType()
                e= "These are the activities' types"
                l.append(c_o)
            else:
                e= "Error: objects are not instances of Person, CulturalHeritageObjects or Activity"
            
        return e, l 

def getClear(list): 
    l=[]
    e="Error: empty list"
    for x in list:
        if type(x) == Person:
            d=dict()
            d["Name"]= x.getName()
            d["Id"]=x.getId()
            e=" Here's the people's list"
            l.append(d)
            
   
        elif type(x)== CulturalHeritageObject or type(x) in CulturalHeritageObject.__subclasses__():
            d=dict()
            d["Title"]= x.getTitle()
            d["Id"]= x.getId()
            d["Type"]= x.getType()
            for pers in x.getAuthors():
                print(x.getAuthors())
                d["Author"]= pers.getName()
            d["Date"]= x.getDate()
            d["Place"]= x.getPlace()
            d["Owner"]= x.getOwner()
            e=" Here's the objects' list"
            l.append(d)
        
        elif type(x)== Activity or type(x) in Activity.__subclasses__():
            d=dict()
            d["Type"]= x.getType()
            d["Responsible Institute"]= x.getResponsibleInstitute()
            d["Responsible Person"]= x.getResponsiblePerson()
            d["Start Date"]= x.getStartDate()
            d["End Date"]= x.getEndDate()
            d["Tool"]= x.getTools()
            d["Refers To"]= (x.refersTo()).getTitle()
            if type(x.refersTo()) != str:
                d["Object Id"]= str((x.refersTo()).getId())
            e= " Here's the activities' list" 
            l.append(d)
              
        else:
            e= "Error: objects are not instances of Person, CulturalHeritageObjects or Activity"
            return e
    return e, l