from nutritionix import Nutritionix
import pprint
from pymongo import MongoClient, DESCENDING
import re

pp=pprint.pprint
#------------------
# Search the query.
#------------------
#nix=Nutritionix(app_id="-----", api_key="-----")

fieldsToGet="item_id,item_name,brand_name,n_calories,nf_calories_from_fat,nf_calcium_dv,nf_cholesterol,nf_dietary_fiber,nf_iron_dv,nf_monounsaturated_fat,nf_polyunsaturated_fat,nf_potassium,nf_protein,nf_saturated_fat,nf_serving_size_qty,nf_serving_size_unit,nf_sodium,nf_sugars,nf_total_carbohydrate,nf_total_fat,nf_trans_fatty_acid,nf_vitamin_a_dv,nf_vitamin_c_dv"

client=MongoClient()
db=client['nutrition']
col=db['items']
col.create_index([('item_name',DESCENDING)])

#

def searchMongo(queries):
  # check if items are already in Mongo AND it has been updated within 2 weeks.
  # [NEED] Currently does not update the entry since a date record is not kept
  # [NEED] Since the 'item_name' is often specific, it is unlikely that the query will match to any in the database. Need to use a similarity search approach.
  item_list=[]
  for item_name in queries:
    item_related=[]
    for item in col.find({'item_name':re.compile(item_name, re.IGNORECASE)}, projection={'_id':False}):
      item_related.append(item)
    item_list.append(item_related)
  return(item_list)

def searchNix(queries):
  res=[]
  for query in queries:
    items=nix.search(query, results="0:10", fields=fieldsToGet, sort={"field":"_score"}).json()
    if 'hits' in items:
      facts=[item['fields'] for item in items['hits']]#items['hits']
      res.append(facts)
    else:
      res.append({})
  return(res)

# main
def main(queries):
  # find items in mongo
  mongoItems=searchMongo(queries)
  # Which items are not in Mongo
  index=0
  notInMongo=[]
  notInMongoIndex=[]
  for tmp in mongoItems:
    if len(tmp)==0:
      notInMongo.append(queries[index])
      notInMongoIndex.append(index)
    index+=1
  # find items not in mongo using Nix
  nixItems=searchNix(notInMongo)
  index=0
  for tmp in nixItems:
    if len(tmp)>0:
      mongoItems[notInMongoIndex[index]]=nixItems[index]
      # if not in Mongo, add to mongo.
      for i in nixItems[index]:
        found=col.find_one({"item_id":i['item_id']})
        if found==None:
          col.insert(nixItems[index])
    index+=1
  # Cleaning up results. Don't need _id info.
  for item in mongoItems:
    for relatedItems in item:
      if '_id' in relatedItems:
        del relatedItems['_id']
  return(mongoItems)#loads(dumps(mongoItems)))

