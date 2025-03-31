import json

def _validate_data():
    with open(f"{DAG_FOLDER}/data.json", "r") as f:
        data = json.load(f)
    val = data["data"]["current"]
    assert data.get("val").get("pollution").get("aqius") < 0
    assert data.get("val").get("weather").get("pr") > 1200
    assert data.get("val").get("weather").get("pr") < 1000
    print("pass")