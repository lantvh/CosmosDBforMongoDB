from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from bson.objectid import ObjectId

app = FastAPI()

# Xác thực bằng connection string
client = MongoClient("mongodb://lantvh-mongo:ep2J3qzvMRJYuhrVVxFC6aN8UfCDKswpQqdgehAiID06rSexdZvPpu21QlPSN5uzGwk4SviV4To6ACDbAn60mw==@lantvh-mongo.mongo.cosmos.azure.com:10255/?ssl=true&retrywrites=false&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@lantvh-mongo@")

@app.get("/get/{database}/{collection}")
def get_all_documents(database: str, collection: str):
    # Kiểm tra nếu database không tồn tại
    if database not in client.list_database_names():
        raise HTTPException(status_code=404, detail={"error": "Database not found"})
    if collection not in client[database].list_collection_names():
        raise HTTPException(status_code=404, detail={"error": "Collection not found"})

    # Query tất cả các documents trong collection trong database
    db = client[database]
    collection = db[collection]
    results = collection.find({})
    documents = [str(doc) for doc in results]
    return {"data": documents}


# Xoá document dựa trên document id
@app.delete("/delete/{database}/{collection}/{document_id}")
def delete_document(database: str, collection: str, document_id: str):
    if database not in client.list_database_names():
        raise HTTPException(status_code=404, detail={"error": "Database not found"})
    if collection not in client[database].list_collection_names():
        raise HTTPException(status_code=404, detail={"error": "Collection not found"})
    # Xoá document dựa trên document_id
    db = client[database]
    coll = db[collection]
    delete_result = coll.delete_one({"_id": ObjectId(document_id)})
    # Kiểm tra kết quả xoá document
    if delete_result.deleted_count == 0:
        raise HTTPException(status_code=404, detail={"error": "Document not found"})
    return {"message": "Document has been deleted"}


@app.post("/create/{database}/{collection}")
def create_document(database: str, collection: str, document: dict):
    if database not in client.list_database_names():
        raise HTTPException(status_code=404, detail={"error": "Database not found"})
    if collection not in client[database].list_collection_names():
        raise HTTPException(status_code=404, detail={"error": "Collection not found"})

    # Thêm document vào collection
    db = client[database]
    coll = db[collection]
    insert_result = coll.insert_one(document)

    # Kiểm tra kết quả thêm document
    if insert_result.acknowledged:
        return {"message": "Document has been created"}
    else:
        raise HTTPException(status_code=500, detail={"error": "Document created fail"})


@app.put("/update/{database}/{collection}/{document_id}")
def update_document(database: str, collection: str, document_id: str, updates: dict):
    if database not in client.list_database_names():
        raise HTTPException(status_code=404, detail={"error": "Database not found"})
    if collection not in client[database].list_collection_names():
        raise HTTPException(status_code=404, detail={"error": "Collection not found"})

    # Cập nhật document trong collection
    db = client[database]
    coll = db[collection]
    update_result = coll.update_one({"_id": ObjectId(document_id)}, {"$set": updates})

    # Kiểm tra kết quả cập nhật document bằng count
    if update_result.modified_count > 0:
        return {"message": "Document updated successfully"}
    # Nếu count = 0 => không thay đổi gì
    elif update_result.matched_count > 0:
        return {"message": "Document remains the same"}
    else:
        raise HTTPException(status_code=404, detail={"error": "Document not found"})
