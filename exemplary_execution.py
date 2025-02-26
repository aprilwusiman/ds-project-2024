from impl import Handler, QueryHandler, MetadataQueryHandler, MetadataUploadHandler
from impl import ProcessDataQueryHandler, ProcessDataUploadHandler
from impl import IdentifiableEntity, Person, CulturalHeritageObject, Activity,AdvancedMashup, BasicMashup, Acquisition, getClear, getLabelList



# Once all the classes are imported, first create the relational
# database using the related source data
rel_path = r"C:/Users/j/Desktop/test/relational.db"
process = ProcessDataUploadHandler() #this gave mistake, so i deleted the db_path as argument
process.setDbPathOrUrl(rel_path)
p=process.pushDataToDb(r"C:/Users/j/Desktop/test/process.json")
# Please remember that one could, in principle, push one or more files
# calling the method one or more times (even calling the method twice
# specifying the same file!)

# Then, create the graph database (remember first to run the
# Blazegraph instance) using the related source data
grp_endpoint = 'http://127.0.0.1:9999/blazegraph/sparql'
metadata = MetadataUploadHandler()
metadata.setDbPathOrUrl(grp_endpoint)
metadata.pushDataToDb(r"C:/Users/j/Desktop/test/meta.csv")
# Please remember that one could, in principle, push one or more files
# calling the method one or more times (even calling the method twice
# specifying the same file!)

# In the next passage, create the query handlers for both
# the databases, using the related classes
process_qh = ProcessDataQueryHandler()
process_qh.setDbPathOrUrl(rel_path)

metadata_qh = MetadataQueryHandler()
metadata_qh.setDbPathOrUrl(grp_endpoint)

# Finally, create a advanced mashup object for asking
# about data
mashup = AdvancedMashup()
mashup.cleanMetadataHandlers()
mashup.cleanProcessHandlers()


mashup.addProcessHandler(process_qh)
mashup.addMetadataHandler(metadata_qh)

q1= mashup.getAuthorsOfObjectsAcquiredInTimeFrame("2023-05-08", "2023-05-15")

q2= mashup.getObjectsHandledByResponsibleInstitution("Council")

q3=mashup.getObjectsHandledByResponsiblePerson("Liddell")

q4= mashup.getAcquisitionsByTechnique("Photo")

q5= mashup.getEntityById("12")

q6= mashup.getAllPeople()

q7= mashup.getAllCulturalHeritageObjects()

q8= mashup.getAuthorsOfCulturalHeritageObject("8")

q9=mashup.getCulturalHeritageObjectsAuthoredBy("VIAF:100219162")

q10= mashup.getAllActivities()

q11= mashup.getActivitiesByResponsibleInstitution("Cou")

q12= mashup.getActivitiesByResponsiblePerson("Alice")

q13=mashup.getActivitiesUsingTool("Blender")

q14= mashup.getActivitiesStartedAfter("2023-05-08")

q15=mashup.getActivitiesEndedBefore("2023-05-15")

q16= mashup.getAcquisitionsByTechnique("Photo")

q17= process_qh.getById("12")

