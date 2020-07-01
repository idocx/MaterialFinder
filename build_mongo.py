from pymongo import MongoClient, HASHED, UpdateOne
from tqdm import tqdm
from utils import BulkHelper, Accumulater
from rdkit.Chem.rdMolDescriptors import CalcMolFormula
from rdkit import Chem


db = MongoClient()["materials"]

collection = db["compounds"]
collection.create_index([('uid', HASHED)])

src_name = "pc"


def make_uid(source, id_):
    return "_" + source + "__" + id_


counter = tqdm(desc="Writing title to Mongo ", unit="piece")

bkh = BulkHelper(100000, lambda e: collection.bulk_write(e))

with open(r"rsc/CID-Title", "r", encoding="latin-1") as f:
    try:
        while 1:
            counter.update()
            new_line = next(f).strip("\n")
            id_string, title = new_line.split("\t")
            uid = make_uid(src_name, id_string)
            bkh.append(
                UpdateOne({
                    "uid": uid
                }, {"$set": {
                    "title": title,
                    "_sync": False
                }}, upsert=True)
            )
    except StopIteration:
        pass

bkh.close()


counter = tqdm(desc="Writing SMILES and formulas to Mongo ", unit="piece")

bkh = BulkHelper(100000, lambda e: collection.bulk_write(e))

with open(r"rsc/CID-SMILES", "r", encoding="latin-1") as f:
    try:
        while 1:
            counter.update()
            new_line = next(f).strip("\n")
            id_string, smiles = new_line.split("\t")
            uid = make_uid(src_name, id_string)
            bkh.append(
                UpdateOne({
                    "uid": uid
                }, {"$set": {
                    "smiles": smiles,
                    "_sync": False
                }}, upsert=True)
            )
    except StopIteration:
        pass

bkh.close()


counter = tqdm(desc="Writing synonyms to Mongo ", unit="piece")

bkh = BulkHelper(50000, lambda e: collection.bulk_write(e))
acc = Accumulater(callback=lambda _id, e: bkh.append(
    UpdateOne(
        {"uid": _id}, {"$addToSet": {"synonyms": {"$each": e}}, "$set": {"_sync": False}}, upsert=True
    )))

with open(r"rsc/CID-Synonym-filtered", "r", encoding="latin-1") as f:
    try:
        while 1:
            counter.update()
            new_line = next(f).strip("\n")
            id_string, name = new_line.split("\t")
            uid = make_uid(src_name, id_string)
            acc.append(uid, name)
    except StopIteration:
        pass

acc.close()
bkh.close()

bkh = BulkHelper(100000, lambda e: collection.bulk_write(e))

for cpd in tqdm(collection.find(
        {"formula": {"$exists": False}, "smiles": {"$exists": True}},
        projection=["smiles"]
)):
    _id = cpd["_id"]
    smiles = cpd.get("smiles", None)
    if smiles is None:
        continue

    mol = Chem.MolFromSmiles(smiles) or Chem.MolFromSmiles(smiles, sanitize=False)
    try:
        formula = CalcMolFormula(mol)
    except RuntimeError:
        continue

    bkh.append(
        UpdateOne({
            "_id": _id
        }, {"$set": {
            "formula": formula,
            "_sync": False
        }})
    )

bkh.close()
